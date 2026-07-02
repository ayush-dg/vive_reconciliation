"""
tests/test_generator.py

Validates src/mock_erp/generator.py against the REAL
config/mock_erp/astech_scenarios.json -- if that config drifts, this test
catches whether the generator still behaves correctly against it.
"""

import sys
import os
import json
from datetime import date
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.mock_erp.generator import generate_mock_erp

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "mock_erp", "astech_scenarios.json")

with open(CONFIG_PATH) as f:
    SCENARIOS_CONFIG = json.load(f)


def _invoices(n):
    return [
        {
            "vendor_id": "ASTECH",
            "vendor_name": "Repairify, Inc dba asTech",
            "shop": "Collex Auto Body",
            "invoice_number": f"SIN{i:05d}",
            "invoice_date": date(2026, 5, 1),
            "ro_number": f"RO-{i}",
            "work_order_number": f"{10000000 + i}",
            "outstanding_amount": "100.00",
            "statement_id": "ASTECH-COLLEX-2026-05",
            "statement_period": "2026-05",
        }
        for i in range(n)
    ]


def test_one_manifest_row_per_statement_invoice():
    invoices = _invoices(202)
    result = generate_mock_erp(invoices, SCENARIOS_CONFIG, seed=SCENARIOS_CONFIG["random_seed"])
    assert len(result.manifest_records) == 202
    manifest_invoice_numbers = {m["statement_invoice_number"] for m in result.manifest_records}
    assert manifest_invoice_numbers == {inv["invoice_number"] for inv in invoices}


def test_expected_bronze_row_count_matches_actual_emission():
    invoices = _invoices(202)
    result = generate_mock_erp(invoices, SCENARIOS_CONFIG, seed=SCENARIOS_CONFIG["random_seed"])
    assert result.expected_bronze_row_count == len(result.bronze_records)


def test_missing_invoice_scenario_produces_zero_bronze_rows_but_one_manifest_row():
    invoices = _invoices(202)
    result = generate_mock_erp(invoices, SCENARIOS_CONFIG, seed=SCENARIOS_CONFIG["random_seed"])
    missing_manifest_rows = [m for m in result.manifest_records if m["scenario_type"] == "missing_invoice"]
    assert len(missing_manifest_rows) > 0
    missing_invoice_numbers = {m["statement_invoice_number"] for m in missing_manifest_rows}
    bronze_invoice_numbers = {r["invoice_number"] for r in result.bronze_records}
    assert missing_invoice_numbers.isdisjoint(bronze_invoice_numbers)
    for m in missing_manifest_rows:
        assert m["generated_erp_invoice_number"] is None


def test_duplicate_invoice_scenario_produces_two_bronze_rows_one_manifest_row():
    invoices = _invoices(202)
    result = generate_mock_erp(invoices, SCENARIOS_CONFIG, seed=SCENARIOS_CONFIG["random_seed"])
    duplicate_manifest_rows = [m for m in result.manifest_records if m["scenario_type"] == "duplicate_invoice"]
    assert len(duplicate_manifest_rows) > 0
    for m in duplicate_manifest_rows:
        matching_bronze = [r for r in result.bronze_records if r["invoice_number"] == m["statement_invoice_number"]]
        assert len(matching_bronze) == 2
        row_numbers = {r["row_number"] for r in matching_bronze}
        assert len(row_numbers) == 2, "duplicate copies must have distinct generation-sequence row_numbers"


def test_posting_date_is_none_exactly_for_pending_rows():
    invoices = _invoices(202)
    result = generate_mock_erp(invoices, SCENARIOS_CONFIG, seed=SCENARIOS_CONFIG["random_seed"])
    for record in result.bronze_records:
        if record["status"] == "PENDING":
            assert record["posting_date"] is None
        else:
            assert record["posting_date"] is not None
            assert record["posting_date"] >= record["invoice_date"]


def test_po_numbers_are_unique_across_the_batch():
    invoices = _invoices(202)
    result = generate_mock_erp(invoices, SCENARIOS_CONFIG, seed=SCENARIOS_CONFIG["random_seed"])
    po_numbers = [r["po_number"] for r in result.bronze_records]
    assert len(po_numbers) == len(set(po_numbers))
    assert all(po.startswith(SCENARIOS_CONFIG["field_generation"]["po_number_prefix"]) for po in po_numbers)


def test_manifest_expected_outcomes_mirror_config_verbatim():
    invoices = _invoices(202)
    result = generate_mock_erp(invoices, SCENARIOS_CONFIG, seed=SCENARIOS_CONFIG["random_seed"])
    for m in result.manifest_records:
        expected = SCENARIOS_CONFIG["expected_outcome_by_scenario"][m["scenario_type"]]
        assert m["expected_match_status"] == expected["match_status"]
        assert m["expected_match_level"] == expected["match_level"]
        assert m["expected_exception_reason"] == expected["exception_reason"]
        assert m["generator_config_version"] == SCENARIOS_CONFIG["generator_version"]


def test_full_run_is_reproducible_for_the_same_seed():
    invoices = _invoices(202)
    first = generate_mock_erp(invoices, SCENARIOS_CONFIG, seed=SCENARIOS_CONFIG["random_seed"])
    second = generate_mock_erp(invoices, SCENARIOS_CONFIG, seed=SCENARIOS_CONFIG["random_seed"])
    assert first.bronze_records == second.bronze_records
    assert first.manifest_records == second.manifest_records
    assert first.expected_bronze_row_count == second.expected_bronze_row_count


def test_no_ai_or_network_dependency():
    # Sanity check on the module's own imports -- this phase must not
    # depend on src/ai/ or any network-touching code at all.
    import src.mock_erp.generator as generator_module
    source = open(generator_module.__file__).read()
    assert "src.ai" not in source
    assert "gemini" not in source.lower()


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
