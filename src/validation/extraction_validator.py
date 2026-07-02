"""
extraction_validator.py

Purpose
-------
The deterministic gate between AI extraction and Bronze. Runs on every
extracted record before it's allowed anywhere near
bronze_vendor_statement_raw. Every rule comes from
config/validation/extraction_rules.json -- nothing vendor- or
provider-specific is hardcoded here.

This module has NO dependency on src/ai/ at all. It operates purely on
already-parsed dicts, on purpose: the same validation rules apply
whether a record came from Gemini, a future Azure OpenAI adapter, or
the legacy pdfplumber fallback -- all of them are expected to produce
dicts shaped like the standard extraction schema before reaching this
gate. Validation doesn't care how you got here.

Design decision worth flagging explicitly: a MISSING confidence score
is NOT treated as invalid. Not every provider (and not the pdfplumber
fallback at all) supplies one, and rejecting on the absence of a field
that's explicitly optional would undermine "gracefully handle providers
that don't supply all metadata." A record with no confidence score
passes on structural validity alone.
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ValidationResult:
    is_valid: bool
    category: Optional[str] = None      # None when is_valid=True; else e.g. 'MISSING_MANDATORY_FIELD'
    details: str = ""
    checked_confidence: Optional[float] = None
    confidence_threshold: Optional[float] = None


def validate_extraction(record: dict, rules: dict) -> ValidationResult:
    """
    Runs structural checks first (missing fields, wrong types) -- if any
    fail, returns immediately without evaluating confidence, since
    evaluating the confidence of a field that isn't even present or
    well-formed doesn't mean anything. Only a structurally sound record
    proceeds to the confidence check.
    """
    required_fields = rules.get("required_fields", [])
    numeric_fields = rules.get("numeric_fields", [])
    date_fields = rules.get("date_fields", [])
    confidence_threshold = rules.get("confidence_threshold")

    missing = [f for f in required_fields if record.get(f) in (None, "")]
    if missing:
        return ValidationResult(
            is_valid=False, category="MISSING_MANDATORY_FIELD",
            details=f"Missing required field(s): {', '.join(missing)}",
        )

    for field in numeric_fields:
        value = record.get(field)
        if value is not None:
            try:
                float(value)
            except (TypeError, ValueError):
                return ValidationResult(
                    is_valid=False, category="INVALID_FIELD_TYPE",
                    details=f"Field '{field}' is not numeric: {value!r}",
                )

    for field in date_fields:
        value = record.get(field)
        if value is not None and not _looks_like_a_date(str(value)):
            return ValidationResult(
                is_valid=False, category="INVALID_FIELD_TYPE",
                details=f"Field '{field}' does not look like a date: {value!r}",
            )

    confidence = record.get("extraction_confidence")
    if confidence is not None and confidence_threshold is not None:
        if float(confidence) < float(confidence_threshold):
            return ValidationResult(
                is_valid=False, category="LOW_CONFIDENCE",
                details=f"extraction_confidence {confidence} is below threshold {confidence_threshold}",
                checked_confidence=float(confidence), confidence_threshold=float(confidence_threshold),
            )

    return ValidationResult(
        is_valid=True,
        checked_confidence=float(confidence) if confidence is not None else None,
        confidence_threshold=float(confidence_threshold) if confidence_threshold is not None else None,
    )


def find_duplicate_indices(records: list[dict], key_fields: list[str]) -> list[int]:
    """
    Returns the INDEX of every record after the first one sharing the
    same key_fields values -- the first occurrence is treated as valid,
    later ones as duplicates. Operates on the whole batch, unlike
    validate_extraction which checks one record at a time.
    """
    seen = set()
    duplicate_indices = []
    for i, record in enumerate(records):
        key = tuple(record.get(f) for f in key_fields)
        if key in seen:
            duplicate_indices.append(i)
        else:
            seen.add(key)
    return duplicate_indices


def _looks_like_a_date(value: str) -> bool:
    """
    Deliberately loose: confirms the string plausibly CONTAINS a date
    shape, not that it parses cleanly. Real parsing against the vendor's
    configured date_format happens in Silver -- this is just a sanity
    check to catch "this is obviously not a date" before Bronze.
    """
    return bool(re.search(r"\d{1,4}[/\-]\d{1,2}[/\-]\d{1,4}", value))
