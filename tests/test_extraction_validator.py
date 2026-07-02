"""
tests/test_extraction_validator.py

Validates src/validation/extraction_validator.py against every rejection
category it's responsible for, plus the deliberate design decision that
a missing (not low) confidence score does not cause rejection.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.validation.extraction_validator import validate_extraction, find_duplicate_indices

RULES = {
    "required_fields": ["vendor", "invoice_number", "amount", "outstanding_amount", "invoice_date", "shop"],
    "numeric_fields": ["amount", "outstanding_amount"],
    "date_fields": ["invoice_date", "due_date"],
    "confidence_threshold": 0.75,
}

GOOD_RECORD = {
    "vendor": "ASTECH", "invoice_number": "SIN12200241", "amount": 48.75,
    "outstanding_amount": 48.75, "invoice_date": "05/01/2026", "shop": "Collex Auto Body",
    "extraction_confidence": 0.95,
}


def test_valid_record_passes():
    result = validate_extraction(GOOD_RECORD, RULES)
    assert result.is_valid is True
    assert result.category is None
    assert result.checked_confidence == 0.95


def test_missing_mandatory_field_is_rejected():
    record = dict(GOOD_RECORD)
    del record["invoice_number"]
    result = validate_extraction(record, RULES)
    assert result.is_valid is False
    assert result.category == "MISSING_MANDATORY_FIELD"
    assert "invoice_number" in result.details


def test_empty_string_counts_as_missing():
    record = dict(GOOD_RECORD, vendor="")
    result = validate_extraction(record, RULES)
    assert result.is_valid is False
    assert result.category == "MISSING_MANDATORY_FIELD"


def test_non_numeric_amount_is_rejected():
    record = dict(GOOD_RECORD, amount="not-a-number")
    result = validate_extraction(record, RULES)
    assert result.is_valid is False
    assert result.category == "INVALID_FIELD_TYPE"
    assert "amount" in result.details


def test_malformed_date_is_rejected():
    record = dict(GOOD_RECORD, invoice_date="not a date at all")
    result = validate_extraction(record, RULES)
    assert result.is_valid is False
    assert result.category == "INVALID_FIELD_TYPE"


def test_low_confidence_is_rejected_when_structurally_valid():
    record = dict(GOOD_RECORD, extraction_confidence=0.42)
    result = validate_extraction(record, RULES)
    assert result.is_valid is False
    assert result.category == "LOW_CONFIDENCE"
    assert result.checked_confidence == 0.42
    assert result.confidence_threshold == 0.75


def test_missing_confidence_does_not_cause_rejection():
    # Deliberate design decision: absence of an optional field the
    # provider didn't supply is not the same as a low-quality extraction.
    record = dict(GOOD_RECORD)
    del record["extraction_confidence"]
    result = validate_extraction(record, RULES)
    assert result.is_valid is True
    assert result.checked_confidence is None


def test_structural_failure_checked_before_confidence():
    # A record that's BOTH missing a field AND low-confidence should be
    # rejected for the structural reason, not silently pass the
    # structural check and only get caught on confidence.
    record = dict(GOOD_RECORD, extraction_confidence=0.10)
    del record["shop"]
    result = validate_extraction(record, RULES)
    assert result.is_valid is False
    assert result.category == "MISSING_MANDATORY_FIELD"


def test_find_duplicate_indices_flags_only_repeats():
    records = [
        {"vendor": "ASTECH", "invoice_number": "SIN1", "amount": 48.75},
        {"vendor": "ASTECH", "invoice_number": "SIN2", "amount": 101.21},
        {"vendor": "ASTECH", "invoice_number": "SIN1", "amount": 48.75},  # duplicate of index 0
        {"vendor": "ASTECH", "invoice_number": "SIN1", "amount": 48.75},  # also duplicate of index 0
    ]
    duplicates = find_duplicate_indices(records, ["vendor", "invoice_number", "amount"])
    assert duplicates == [2, 3]


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
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
