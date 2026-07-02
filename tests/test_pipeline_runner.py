"""
tests/test_pipeline_runner.py

Validates src/pipeline/runner.py's sequencing logic entirely through
injected fake executor/table_counter callables -- no Spark, no real
notebooks, matching the style already used for GeminiClient (fake
transport) and ExtractionService (fake AIClient).
"""

import sys
import os
import tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.pipeline.runner import (
    Stage,
    StageResult,
    PipelineStageFailed,
    run_pipeline,
    default_table_counter,
    default_notebook_executor,
    PIPELINE_STAGES,
)


def _stages(*names):
    return [Stage(name, f"{name}.py") for name in names]


def test_runs_every_stage_in_order():
    calls = []

    def executor(path, g):
        calls.append(path)

    results = run_pipeline(_stages("A", "B", "C"), {}, executor=executor, table_counter=lambda spark, tables: {})

    assert calls == ["A.py", "B.py", "C.py"]
    assert all(r.success for r in results)
    assert len(results) == 3


def test_stops_immediately_on_first_failure():
    calls = []

    def executor(path, g):
        calls.append(path)
        if path == "B.py":
            raise RuntimeError("boom")

    try:
        run_pipeline(_stages("A", "B", "C"), {}, executor=executor, table_counter=lambda spark, tables: {})
        assert False, "expected PipelineStageFailed"
    except PipelineStageFailed as e:
        assert e.stage.name == "B"
        assert "boom" in str(e)

    # C's executor must never have been called
    assert calls == ["A.py", "B.py"]


def test_failure_result_is_recorded_before_raising():
    def executor(path, g):
        if path == "A.py":
            raise ValueError("nope")

    try:
        run_pipeline(_stages("A", "B"), {}, executor=executor, table_counter=lambda spark, tables: {})
        assert False, "expected PipelineStageFailed"
    except PipelineStageFailed as e:
        assert e.original.__class__ is ValueError


def test_exec_globals_dict_is_shared_and_mutated_across_stages():
    shared = {}

    def executor(path, g):
        assert g is shared  # literally the same object every stage
        g.setdefault("seen", []).append(path)

    run_pipeline(_stages("A", "B", "C"), shared, executor=executor, table_counter=lambda spark, tables: {})

    assert shared["seen"] == ["A.py", "B.py", "C.py"]


def test_table_counter_invoked_with_each_stage_own_table_list():
    stages = [
        Stage("A", "a.py", tables_to_report=["t1"]),
        Stage("B", "b.py", tables_to_report=["t2", "t3"]),
    ]
    seen_calls = []

    def table_counter(spark, tables):
        seen_calls.append(list(tables))
        return {t: 1 for t in tables}

    results = run_pipeline(stages, {}, executor=lambda p, g: None, table_counter=table_counter)

    assert seen_calls == [["t1"], ["t2", "t3"]]
    assert results[0].table_counts == {"t1": 1}
    assert results[1].table_counts == {"t2": 1, "t3": 1}


def test_default_table_counter_handles_query_errors_gracefully():
    class ExplodingSpark:
        def table(self, name):
            raise RuntimeError(f"no such table: {name}")

    counts = default_table_counter(ExplodingSpark(), ["missing_table"])
    assert counts == {"missing_table": None}


def test_default_table_counter_reports_real_counts():
    class FakeDF:
        def __init__(self, n):
            self.n = n

        def count(self):
            return self.n

    class FakeSpark:
        def table(self, name):
            return FakeDF({"x": 3, "y": 7}[name])

    counts = default_table_counter(FakeSpark(), ["x", "y"])
    assert counts == {"x": 3, "y": 7}


def test_default_notebook_executor_shares_state_via_real_files():
    # Two tiny real .py files -- proves the exec()/shared-namespace
    # mechanism itself works, independent of Spark or the production
    # notebooks.
    with tempfile.TemporaryDirectory() as tmp:
        script_a = os.path.join(tmp, "a.py")
        script_b = os.path.join(tmp, "b.py")
        with open(script_a, "w") as f:
            f.write("shared_value = 42\n")
        with open(script_b, "w") as f:
            f.write("result = shared_value + 1\n")  # only works if b.py sees a.py's globals

        exec_globals = {}
        default_notebook_executor(script_a, exec_globals)
        default_notebook_executor(script_b, exec_globals)

        assert exec_globals["result"] == 43


def test_default_notebook_executor_propagates_exceptions():
    with tempfile.TemporaryDirectory() as tmp:
        script = os.path.join(tmp, "broken.py")
        with open(script, "w") as f:
            f.write("raise ValueError('deliberately broken')\n")

        try:
            default_notebook_executor(script, {})
            assert False, "expected ValueError to propagate"
        except ValueError as e:
            assert "deliberately broken" in str(e)


def test_pipeline_stages_cover_the_full_implemented_flow_in_order():
    names = [s.name for s in PIPELINE_STAGES]
    paths = [s.notebook_path for s in PIPELINE_STAGES]
    assert paths == [
        "notebooks/00_setup_lakehouse_schema.py",
        "notebooks/01_bronze_ingestion.py",
        "notebooks/02_silver_normalization_statement.py",
        "notebooks/03_mock_erp_generator.py",
        "notebooks/04_silver_normalization_erp.py",
    ]
    assert len(names) == 5


def test_stage_result_default_table_counts_is_independent_per_instance():
    # dataclass field(default_factory=dict) guard -- two StageResults must
    # not share the same underlying dict.
    r1 = StageResult(stage=Stage("A", "a.py"), success=True, elapsed_seconds=0.1)
    r2 = StageResult(stage=Stage("B", "b.py"), success=True, elapsed_seconds=0.1)
    r1.table_counts["x"] = 1
    assert r2.table_counts == {}


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed, failed = 0, 0
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"FAIL  {t.__name__}  -- {e}")
            failed += 1
        except Exception as e:
            print(f"ERROR {t.__name__}  -- {type(e).__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
