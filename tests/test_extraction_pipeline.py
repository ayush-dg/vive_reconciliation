"""
tests/test_extraction_pipeline.py

Validates src/ai/extraction_pipeline.py -- the glue between ExtractionService
and the existing validation / review-queue / audit-log layers. No Spark, no
network, no PDF: ExtractionOutcome/AIResponse objects are constructed by
hand, exactly the shape ExtractionService would have produced.
"""

import sys
import os
import json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ai.base_client import AIResponse
from src.ai.extraction_service import ExtractionOutcome
from src.ai.extraction_pipeline import (
    standardize_record,
    process_page,
    deduplicate_batch,
)

VALIDATION_RULES = {
    "required_fields": ["vendor", "invoice_number", "amount", "outstanding_amount", "invoice_date", "shop"],
    "numeric_fields": ["amount", "outstanding_amount"],
    "date_fields": ["invoice_date", "due_date"],
    "confidence_threshold": 0.75,
}

CONTEXT = dict(
    vendor_id="ASTECH", source_file="astech_vendor_statement_may2026.pdf",
    statement_id="ASTECH-COLLEX-2026-05", statement_period="2026-05",
)


def _success_outcome(records, text="raw gemini text"):
    response = AIResponse(success=True, text=text, model="gemini-2.0-flash", latency_ms=100.0, attempt_count=1)
    return ExtractionOutcome(ai_response=response, records=records)


# ---- standardize_record ---------------------------------------------------

def test_standardize_injects_vendor_and_shop():
    record = standardize_record({"invoice_number": "SIN1"}, vendor_id="ASTECH", shop="Collex Auto Body")
    assert record["vendor"] == "ASTECH"
    assert record["shop"] == "Collex Auto Body"


def test_standardize_renames_confidence_key():
    record = standardize_record({"confidence": 0.9}, vendor_id="ASTECH", shop="Collex")
    assert record["extraction_confidence"] == 0.9
    assert "confidence" not in record


def test_standardize_derives_amount_when_configured():
    record = standardize_record(
        {"outstanding_amount": "48.75"}, vendor_id="ASTECH", shop="Collex",
        derive_amount_from_outstanding=True,
    )
    assert record["amount"] == "48.75"


def test_standardize_does_not_derive_amount_when_not_configured():
    record = standardize_record(
        {"outstanding_amount": "48.75"}, vendor_id="ASTECH", shop="Collex",
        derive_amount_from_outstanding=False,
    )
    assert record.get("amount") is None


def test_standardize_does_not_overwrite_explicit_amount():
    record = standardize_record(
        {"outstanding_amount": "48.75", "amount": "60.00"}, vendor_id="ASTECH", shop="Collex",
        derive_amount_from_outstanding=True,
    )
    assert record["amount"] == "60.00"


def test_standardize_normalizes_numeric_formatting():
    record = standardize_record({"amount": "48.7", "outstanding_amount": "48.750"}, vendor_id="ASTECH", shop="Collex")
    assert record["amount"] == "48.70"
    assert record["outstanding_amount"] == "48.75"


def test_standardize_leaves_non_numeric_amount_untouched_for_validator_to_catch():
    record = standardize_record({"amount": "not-a-number"}, vendor_id="ASTECH", shop="Collex")
    assert record["amount"] == "not-a-number"


def test_standardize_does_not_mutate_input():
    original = {"confidence": 0.9, "invoice_number": "SIN1"}
    standardize_record(original, vendor_id="ASTECH", shop="Collex")
    assert original == {"confidence": 0.9, "invoice_number": "SIN1"}


# ---- process_page: happy path ---------------------------------------------

def test_process_page_all_valid_records_go_to_bronze():
    outcome = _success_outcome([
        {"invoice_date": "05/01/2026", "invoice_number": "SIN1", "outstanding_amount": "48.75",
         "due_date": "05/31/2026", "confidence": 0.95},
        {"invoice_date": "05/02/2026", "invoice_number": "SIN2", "outstanding_amount": "101.21",
         "due_date": "05/31/2026", "confidence": 0.9},
    ])
    result = process_page(
        outcome, validation_rules=VALIDATION_RULES, shop="Collex Auto Body",
        page_number=1, derive_amount_from_outstanding=True, **CONTEXT,
    )
    assert len(result.bronze_records) == 2
    assert result.review_queue_records == []
    assert result.bronze_records[0]["vendor"] == "ASTECH"
    assert result.bronze_records[0]["shop"] == "Collex Auto Body"
    assert result.bronze_records[0]["extraction_confidence"] == 0.95
    assert result.bronze_records[0]["page_number"] == 1
    assert result.bronze_records[0]["row_number"] == 1
    assert result.audit_record["validation_result"] == "2/2 valid"
    assert result.audit_record["success"] is True


def test_process_page_partial_failure_splits_records():
    outcome = _success_outcome([
        {"invoice_date": "05/01/2026", "invoice_number": "SIN1", "outstanding_amount": "48.75",
         "due_date": "05/31/2026", "confidence": 0.95},
        {"invoice_date": "05/02/2026", "invoice_number": "SIN2", "outstanding_amount": "101.21",
         "due_date": "05/31/2026", "confidence": 0.10},  # below 0.75 threshold
    ])
    result = process_page(
        outcome, validation_rules=VALIDATION_RULES, shop="Collex Auto Body",
        page_number=1, derive_amount_from_outstanding=True, **CONTEXT,
    )
    assert len(result.bronze_records) == 1
    assert result.bronze_records[0]["invoice_number"] == "SIN1"
    assert len(result.review_queue_records) == 1
    assert result.review_queue_records[0]["rejection_category"] == "LOW_CONFIDENCE"
    assert result.review_queue_records[0]["extraction_confidence"] == 0.10
    assert result.review_queue_records[0]["confidence_threshold_applied"] == 0.75
    assert result.audit_record["validation_result"] == "1/2 valid"
    # every review-queue row from this page traces back to this page's audit record
    assert result.review_queue_records[0]["ai_audit_id"] == result.audit_record["audit_id"]


def test_process_page_missing_mandatory_field_goes_to_review_queue():
    outcome = _success_outcome([
        {"invoice_date": "05/01/2026", "outstanding_amount": "48.75", "due_date": "05/31/2026", "confidence": 0.95},
    ])
    result = process_page(
        outcome, validation_rules=VALIDATION_RULES, shop="Collex Auto Body",
        page_number=1, derive_amount_from_outstanding=True, **CONTEXT,
    )
    assert result.bronze_records == []
    assert result.review_queue_records[0]["rejection_category"] == "MISSING_MANDATORY_FIELD"
    assert "invoice_number" in result.review_queue_records[0]["rejection_details"]


def test_process_page_audit_record_carries_average_confidence():
    outcome = _success_outcome([
        {"invoice_date": "05/01/2026", "invoice_number": "SIN1", "outstanding_amount": "48.75",
         "due_date": "05/31/2026", "confidence": 0.9},
        {"invoice_date": "05/02/2026", "invoice_number": "SIN2", "outstanding_amount": "101.21",
         "due_date": "05/31/2026", "confidence": 0.8},
    ])
    result = process_page(
        outcome, validation_rules=VALIDATION_RULES, shop="Collex Auto Body",
        page_number=1, derive_amount_from_outstanding=True, **CONTEXT,
    )
    assert abs(result.audit_record["extraction_confidence"] - 0.85) < 1e-9


def test_process_page_empty_records_produces_zero_zero_summary():
    result = process_page(
        _success_outcome([]), validation_rules=VALIDATION_RULES, shop="Collex Auto Body",
        page_number=3, **CONTEXT,
    )
    assert result.bronze_records == []
    assert result.review_queue_records == []
    assert result.audit_record["validation_result"] == "0/0 valid"


# ---- process_page: failure paths ------------------------------------------

def test_process_page_ai_call_failure_yields_single_review_row():
    failed = AIResponse(success=False, error="HTTP 503 on attempt 3: unavailable", attempt_count=3)
    outcome = ExtractionOutcome(ai_response=failed, records=[])
    result = process_page(
        outcome, validation_rules=VALIDATION_RULES, shop="Collex Auto Body",
        page_number=5, **CONTEXT,
    )
    assert result.bronze_records == []
    assert len(result.review_queue_records) == 1
    assert result.review_queue_records[0]["rejection_category"] == "AI_CALL_FAILED"
    assert "Page 5" in result.review_queue_records[0]["rejection_details"]
    assert result.audit_record["success"] is False
    assert result.review_queue_records[0]["ai_audit_id"] == result.audit_record["audit_id"]


def test_process_page_malformed_json_contract_yields_single_review_row():
    response = AIResponse(success=True, text='{"unexpected": "shape"}', model="gemini-2.0-flash")
    outcome = ExtractionOutcome(ai_response=response, records=[], error="Expected a JSON object shaped like {'invoices': [...]}")
    result = process_page(
        outcome, validation_rules=VALIDATION_RULES, shop="Collex Auto Body",
        page_number=2, **CONTEXT,
    )
    assert result.bronze_records == []
    assert len(result.review_queue_records) == 1
    assert result.review_queue_records[0]["rejection_category"] == "MALFORMED_JSON"
    assert result.audit_record["success"] is True  # the HTTP/JSON call itself succeeded


def test_ai_call_failure_and_malformed_json_are_distinguishable_categories():
    failed = ExtractionOutcome(ai_response=AIResponse(success=False, error="boom"), records=[])
    malformed = ExtractionOutcome(ai_response=AIResponse(success=True, text="{}"), records=[], error="bad shape")

    failed_result = process_page(failed, validation_rules=VALIDATION_RULES, shop="Collex", page_number=1, **CONTEXT)
    malformed_result = process_page(malformed, validation_rules=VALIDATION_RULES, shop="Collex", page_number=1, **CONTEXT)

    assert failed_result.review_queue_records[0]["rejection_category"] != malformed_result.review_queue_records[0]["rejection_category"]


# ---- deduplicate_batch ------------------------------------------------------

def test_deduplicate_batch_flags_exact_repeats():
    records = [
        {"vendor": "ASTECH", "invoice_number": "SIN1", "amount": "48.75"},
        {"vendor": "ASTECH", "invoice_number": "SIN2", "amount": "101.21"},
        {"vendor": "ASTECH", "invoice_number": "SIN1", "amount": "48.75"},
    ]
    kept, review_rows = deduplicate_batch(records, key_fields=["vendor", "invoice_number", "amount"], **CONTEXT)
    assert len(kept) == 2
    assert len(review_rows) == 1
    assert review_rows[0]["rejection_category"] == "DUPLICATE_RECORD"


def test_deduplicate_batch_catches_formatting_variants_after_standardization():
    # "48.75" vs "48.750" would be distinct strings without standardize_record's
    # canonical numeric formatting -- verify the two compose correctly.
    raw = [
        {"invoice_number": "SIN1", "amount": "48.75"},
        {"invoice_number": "SIN1", "amount": "48.750"},
    ]
    standardized = [standardize_record(r, vendor_id="ASTECH", shop="Collex") for r in raw]
    kept, review_rows = deduplicate_batch(standardized, key_fields=["vendor", "invoice_number", "amount"], **CONTEXT)
    assert len(kept) == 1
    assert len(review_rows) == 1


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
