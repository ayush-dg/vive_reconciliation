"""
tests/test_mutations.py

Validates src/mock_erp/mutations.py -- one test group per scenario, plus
reproducibility of the RNG-driven scenarios.
"""

import sys
import os
import random
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.mock_erp.mutations import (
    MUTATORS,
    mutate_exact_match,
    mutate_invoice_revision,
    mutate_missing_invoice,
    mutate_amount_mismatch,
    mutate_duplicate_invoice,
    mutate_vendor_reference_issue,
    mutate_missing_credit,
    mutate_pending_posting,
)

INVOICE = {
    "vendor_id": "ASTECH",
    "vendor_name": "Repairify, Inc dba asTech",
    "shop": "Collex Auto Body",
    "invoice_number": "SIN12200241",
    "invoice_date": "2026-05-01",
    "ro_number": "RO-9001",
    "work_order_number": "24419074",
    "outstanding_amount": "48.75",
    "statement_id": "ASTECH-COLLEX-2026-05",
    "statement_period": "2026-05",
}


def test_exact_match_mirrors_statement():
    result = mutate_exact_match(INVOICE, random.Random(1), {})
    assert len(result.erp_records) == 1
    record = result.erp_records[0]
    assert record["invoice_number"] == "SIN12200241"
    assert record["amount"] == "48.75"
    assert record["outstanding_amount"] == "48.75"
    assert record["ro_number"] == "RO-9001"
    assert result.generated_erp_invoice_number == "SIN12200241"


def test_invoice_revision_appends_a_configured_suffix():
    params = {"suffixes_to_apply": ["-1", "X1", "R"]}
    result = mutate_invoice_revision(INVOICE, random.Random(1), params)
    record = result.erp_records[0]
    assert record["invoice_number"].startswith("SIN12200241")
    suffix = record["invoice_number"][len("SIN12200241"):]
    assert suffix in params["suffixes_to_apply"]
    assert result.generated_erp_invoice_number == record["invoice_number"]
    assert record["amount"] == "48.75"


def test_invoice_revision_is_reproducible_for_same_seed():
    params = {"suffixes_to_apply": ["-1", "X1", "R"]}
    first = mutate_invoice_revision(INVOICE, random.Random(99), params)
    second = mutate_invoice_revision(INVOICE, random.Random(99), params)
    assert first.erp_records == second.erp_records


def test_missing_invoice_emits_nothing():
    result = mutate_missing_invoice(INVOICE, random.Random(1), {})
    assert result.erp_records == []
    assert result.generated_erp_invoice_number is None
    assert "absent" in result.mutation_details.lower()


def test_amount_mismatch_within_configured_variance_and_differs_from_original():
    params = {"variance_min_pct": 0.05, "variance_max_pct": 0.25}
    result = mutate_amount_mismatch(INVOICE, random.Random(2), params)
    record = result.erp_records[0]
    original = 48.75
    mismatched = float(record["amount"])
    assert record["amount"] == record["outstanding_amount"]
    assert mismatched != original
    variance = abs(mismatched - original) / original
    assert 0.05 - 1e-6 <= variance <= 0.25 + 1e-6
    assert record["invoice_number"] == "SIN12200241"


def test_duplicate_invoice_emits_original_plus_configured_extra_copies():
    result = mutate_duplicate_invoice(INVOICE, random.Random(1), {"max_duplicate_copies": 1})
    assert len(result.erp_records) == 2
    assert result.erp_records[0] == result.erp_records[1]
    assert result.erp_records[0]["invoice_number"] == "SIN12200241"
    assert result.generated_erp_invoice_number == "SIN12200241"


def test_duplicate_invoice_respects_configured_copy_count():
    result = mutate_duplicate_invoice(INVOICE, random.Random(1), {"max_duplicate_copies": 3})
    assert len(result.erp_records) == 4  # 1 original + 3 duplicates


def test_vendor_reference_issue_substitutes_real_work_order_number():
    result = mutate_vendor_reference_issue(INVOICE, random.Random(1), {})
    record = result.erp_records[0]
    assert record["invoice_number"] == INVOICE["work_order_number"]
    assert record["invoice_number"] != INVOICE["invoice_number"]
    assert record["ro_number"] is None
    assert result.generated_erp_invoice_number == INVOICE["work_order_number"]
    assert "synthesized" not in result.mutation_details.lower()


def test_vendor_reference_issue_falls_back_to_synthesized_placeholder_when_no_work_order_number():
    invoice_without_wo = dict(INVOICE, work_order_number=None)
    result = mutate_vendor_reference_issue(invoice_without_wo, random.Random(1), {})
    record = result.erp_records[0]
    assert record["invoice_number"] != invoice_without_wo["invoice_number"]
    assert record["ro_number"] is None
    assert "synthesized" in result.mutation_details.lower()


def test_missing_credit_reduces_outstanding_but_not_amount():
    params = {"credit_min_pct": 0.05, "credit_max_pct": 0.15}
    result = mutate_missing_credit(INVOICE, random.Random(3), params)
    record = result.erp_records[0]
    assert record["amount"] == "48.75"
    assert float(record["outstanding_amount"]) < float(record["amount"])
    credit_pct = 1 - (float(record["outstanding_amount"]) / float(record["amount"]))
    assert 0.05 - 1e-6 <= credit_pct <= 0.15 + 1e-6


def test_pending_posting_leaves_amount_and_invoice_number_unchanged():
    result = mutate_pending_posting(INVOICE, random.Random(1), {})
    record = result.erp_records[0]
    assert record["invoice_number"] == "SIN12200241"
    assert record["amount"] == "48.75"
    assert record["outstanding_amount"] == "48.75"


def test_all_scenario_mix_keys_have_a_registered_mutator():
    scenario_keys = [
        "exact_match", "invoice_revision", "missing_invoice", "amount_mismatch",
        "duplicate_invoice", "vendor_reference_issue", "missing_credit", "pending_posting",
    ]
    for key in scenario_keys:
        assert key in MUTATORS, f"no mutator registered for {key!r}"


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
