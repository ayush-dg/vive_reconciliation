"""
tests/test_review_queue.py

Validates src/validation/review_queue.py -- the row shape it builds for
validation_document_review_queue, and its default field values.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.validation.review_queue import build_review_queue_record


def _minimal_record(**overrides):
    base = dict(
        vendor_id="ASTECH",
        source_file="astech_vendor_statement_may2026.pdf",
        statement_id="ASTECH-COLLEX-2026-05",
        statement_period="2026-05",
        pipeline_stage="AI_EXTRACTION",
        rejection_category="LOW_CONFIDENCE",
        rejection_details="extraction_confidence 0.42 is below threshold 0.75",
        raw_payload='{"invoice_number": "SIN1"}',
    )
    base.update(overrides)
    return build_review_queue_record(**base)


def test_builds_expected_row_shape():
    record = _minimal_record()
    expected_keys = {
        "review_id", "vendor_id", "source_file", "statement_id", "statement_period",
        "pipeline_stage", "rejection_category", "rejection_details", "extraction_confidence",
        "confidence_threshold_applied", "raw_payload", "ai_audit_id", "review_status",
        "flagged_timestamp", "reviewed_by", "reviewed_timestamp", "resolution_notes",
    }
    assert set(record.keys()) == expected_keys


def test_defaults_to_pending_review_with_unresolved_fields_null():
    record = _minimal_record()
    assert record["review_status"] == "PENDING_REVIEW"
    assert record["reviewed_by"] is None
    assert record["reviewed_timestamp"] is None
    assert record["resolution_notes"] is None
    assert record["flagged_timestamp"] is not None


def test_generates_unique_surrogate_key_per_call():
    first = _minimal_record()
    second = _minimal_record()
    assert first["review_id"] != second["review_id"]


def test_optional_fields_pass_through_when_provided():
    record = _minimal_record(
        extraction_confidence=0.42,
        confidence_threshold_applied=0.75,
        ai_audit_id="audit-123",
    )
    assert record["extraction_confidence"] == 0.42
    assert record["confidence_threshold_applied"] == 0.75
    assert record["ai_audit_id"] == "audit-123"


def test_optional_fields_default_to_none():
    record = _minimal_record()
    assert record["extraction_confidence"] is None
    assert record["confidence_threshold_applied"] is None
    assert record["ai_audit_id"] is None


def test_statement_id_can_be_null_when_undeterminable():
    record = _minimal_record(statement_id=None)
    assert record["statement_id"] is None


def test_rejection_category_and_details_pass_through_verbatim():
    record = _minimal_record(rejection_category="AI_CALL_FAILED", rejection_details="HTTP 503 on attempt 3")
    assert record["rejection_category"] == "AI_CALL_FAILED"
    assert record["rejection_details"] == "HTTP 503 on attempt 3"


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
