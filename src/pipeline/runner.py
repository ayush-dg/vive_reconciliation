"""
runner.py

Purpose
-------
Executes the implemented pipeline notebooks in order, in ONE shared Python
namespace, so `spark` (created by 00_setup_lakehouse_schema.py's
environment-detection shim) persists across every stage exactly like Fabric
running notebooks in the same session -- no stage after the first creates
its own SparkSession. No notebook logic is reimplemented here; this module
only sequences the existing, unmodified notebook files.

PIPELINE_STAGES is the single source of truth for execution order and
which tables each stage is expected to populate -- both
scripts/run_pipeline.py and any test that needs to reason about "what
stages exist" import it from here rather than re-declaring it.

Design commitment
------------------
`executor` and `table_counter` are injectable, same pattern as
GeminiClient's transport / ExtractionService's AIClient. This is what
makes run_pipeline()'s sequencing and stop-on-first-failure behavior fully
unit-testable without a real Spark session or real notebooks -- see
tests/test_pipeline_runner.py.
"""

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class Stage:
    name: str
    notebook_path: str
    tables_to_report: list = field(default_factory=list)


PIPELINE_STAGES = [
    Stage("Setup Lakehouse Schema", "notebooks/00_setup_lakehouse_schema.py", []),
    Stage(
        "Bronze Ingestion (Gemini Extraction + Validation)",
        "notebooks/01_bronze_ingestion.py",
        ["bronze_vendor_statement_raw", "validation_document_review_queue", "ai_audit_log"],
    ),
    Stage(
        "Silver Normalization -- Vendor Statement",
        "notebooks/02_silver_normalization_statement.py",
        ["silver_reconciliation_standard"],
    ),
    Stage(
        "Mock ERP Generator",
        "notebooks/03_mock_erp_generator.py",
        ["bronze_internal_erp_raw", "validation_mutation_manifest"],
    ),
    Stage(
        "Silver Normalization -- Internal ERP",
        "notebooks/04_silver_normalization_erp.py",
        ["silver_reconciliation_standard"],
    ),
]


class PipelineStageFailed(Exception):
    def __init__(self, stage: Stage, original: Exception):
        self.stage = stage
        self.original = original
        super().__init__(f"Stage '{stage.name}' failed: {original}")


@dataclass
class StageResult:
    stage: Stage
    success: bool
    elapsed_seconds: float
    error: Optional[str] = None
    table_counts: dict = field(default_factory=dict)


def default_notebook_executor(notebook_path: str, exec_globals: dict) -> None:
    """
    Runs one notebook's source in exec_globals -- the same dict every other
    stage runs in, which is what lets `spark` (and anything else a
    notebook binds at module scope, e.g. vendor_config) persist across
    stages.
    """
    with open(notebook_path) as f:
        source = f.read()
    exec(compile(source, notebook_path, "exec"), exec_globals)


def default_table_counter(spark, table_names: list) -> dict:
    """
    Best-effort row count per table. A query failure (table doesn't exist
    yet, e.g. a stage was skipped) is recorded as None rather than raised
    -- this is a reporting helper, not a correctness gate; use
    src/validation/pipeline_checks.py for pass/fail validation.
    """
    counts = {}
    for name in table_names:
        try:
            counts[name] = spark.table(name).count()
        except Exception:
            counts[name] = None
    return counts


def run_pipeline(
    stages=None,
    exec_globals=None,
    executor: Callable = default_notebook_executor,
    table_counter: Callable = default_table_counter,
    on_stage_start: Optional[Callable] = None,
    on_stage_complete: Optional[Callable] = None,
) -> list:
    """
    Runs each stage in order in the SAME exec_globals namespace. Stops
    immediately on the first failing stage -- later stages' executor is
    never called. Returns the list of StageResults completed so far
    (raises PipelineStageFailed after appending the failing stage's
    result, so callers can inspect exactly where it stopped via the
    exception's .stage attribute).
    """
    stages = PIPELINE_STAGES if stages is None else stages
    exec_globals = {} if exec_globals is None else exec_globals
    results = []

    for stage in stages:
        if on_stage_start:
            on_stage_start(stage)
        start = time.monotonic()
        try:
            executor(stage.notebook_path, exec_globals)
        except Exception as e:
            elapsed = time.monotonic() - start
            result = StageResult(stage=stage, success=False, elapsed_seconds=elapsed, error=str(e))
            results.append(result)
            if on_stage_complete:
                on_stage_complete(result)
            raise PipelineStageFailed(stage, e) from e

        elapsed = time.monotonic() - start
        counts = table_counter(exec_globals.get("spark"), stage.tables_to_report)
        result = StageResult(stage=stage, success=True, elapsed_seconds=elapsed, table_counts=counts)
        results.append(result)
        if on_stage_complete:
            on_stage_complete(result)

    return results
