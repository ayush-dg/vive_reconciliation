"""
extraction_pipeline.py

Purpose
-------
The glue between ExtractionService (talks to the AI provider) and the
existing validation / review-queue / audit-log layers. Nothing in this
module is provider-specific and nothing in it touches Spark -- it operates
on plain dicts so it's unit-testable exactly like extraction_validator.py
and audit_logger.py, and reusable if a future provider adapter is added.

Responsibilities, split into three functions on purpose (each independently
testable, each reused rather than duplicated across the AI path and the
reactive pdfplumber fallback path):

- standardize_record: turns one raw provider record into the "standard
  extraction schema" validate_extraction() expects -- injecting context the
  provider was never asked for (vendor, shop), renaming provider-specific
  keys to the validator's expected names (confidence -> extraction_confidence),
  and applying vendor-declared shape facts (e.g. this vendor's statement has
  no separate "amount" from "outstanding_amount" -- config-driven via
  derive_amount_from_outstanding, not hardcoded per-vendor here).
- process_page: runs one page's ExtractionOutcome through
  standardize_record + validate_extraction (both reused, not reimplemented),
  splits into Bronze-bound vs review-queue-bound records, and builds the
  page's ai_audit_log row via build_audit_record (reused).
- deduplicate_batch: runs find_duplicate_indices (reused) across the whole
  document's collected records, once, after all pages are standardized --
  not per page, since duplicates can span pages.
"""

import json
from dataclasses import dataclass, field
from typing import Optional

from .audit_logger import build_audit_record
from .base_client import AIResponse
from .extraction_service import ExtractionOutcome, ExtractionService
from ..validation.extraction_validator import validate_extraction, find_duplicate_indices
from ..validation.review_queue import build_review_queue_record

NUMERIC_FIELDS = ("amount", "outstanding_amount")


def standardize_record(
    record: dict,
    *,
    vendor_id: str,
    shop: Optional[str],
    derive_amount_from_outstanding: bool = False,
) -> dict:
    """
    Maps one raw provider record onto the standard extraction schema
    validate_extraction() checks against. Pure, side-effect-free -- does not
    mutate the input dict.
    """
    standardized = dict(record)
    standardized["vendor"] = vendor_id
    standardized["shop"] = shop

    if "confidence" in standardized:
        standardized["extraction_confidence"] = standardized.pop("confidence")

    if derive_amount_from_outstanding and not standardized.get("amount"):
        standardized["amount"] = standardized.get("outstanding_amount")

    for numeric_field in NUMERIC_FIELDS:
        standardized[numeric_field] = _canonical_numeric_string(standardized.get(numeric_field))

    return standardized


def _canonical_numeric_string(value) -> Optional[str]:
    """
    Normalizes a numeric-looking value to a fixed-precision string
    ("48.75", "48.750", 48.75 all -> "48.75") so that duplicate detection
    (exact tuple equality in find_duplicate_indices) doesn't false-negative
    on formatting differences between extraction methods or pages.
    Non-numeric or missing values pass through unchanged -- validate_extraction
    is responsible for flagging those, not this function.
    """
    if value is None:
        return None
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return value


@dataclass
class PageExtractionResult:
    bronze_records: list = field(default_factory=list)
    review_queue_records: list = field(default_factory=list)
    audit_record: Optional[dict] = None


def process_page(
    outcome: ExtractionOutcome,
    *,
    validation_rules: dict,
    vendor_id: str,
    shop: Optional[str],
    source_file: str,
    statement_id: Optional[str],
    statement_period: str,
    page_number: int,
    derive_amount_from_outstanding: bool = False,
    ai_provider: str = "gemini",
    prompt_version: str = ExtractionService.PROMPT_VERSION,
) -> PageExtractionResult:
    """
    Total-call-failure and malformed-JSON-contract cases short-circuit to
    zero Bronze records + exactly one review-queue row, distinguishing the
    two failure categories (AI_CALL_FAILED vs MALFORMED_JSON) so a reviewer
    triaging the queue knows whether the fix is "retry" or "look at the
    prompt/layout." Both still produce an audit_record -- every AI call is
    logged regardless of outcome.

    A partially-successful page (some records valid, some not) is NOT a
    failure case: valid records go to Bronze, invalid ones to the review
    queue, individually. This is a deliberate scope boundary -- partial
    per-row validation failures are real, new data for human triage, not
    something an automatic fallback should silently paper over.
    """
    if not outcome.ai_response.success:
        return _failed_page_result(
            outcome.ai_response, rejection_category="AI_CALL_FAILED",
            rejection_details=f"Page {page_number}: {outcome.ai_response.error}",
            validation_result="AI_CALL_FAILED",
            vendor_id=vendor_id, source_file=source_file, statement_id=statement_id,
            statement_period=statement_period, ai_provider=ai_provider, prompt_version=prompt_version,
        )

    if outcome.error is not None:
        return _failed_page_result(
            outcome.ai_response, rejection_category="MALFORMED_JSON",
            rejection_details=f"Page {page_number}: {outcome.error}",
            validation_result="MALFORMED_JSON",
            vendor_id=vendor_id, source_file=source_file, statement_id=statement_id,
            statement_period=statement_period, ai_provider=ai_provider, prompt_version=prompt_version,
        )

    bronze_records, review_queue_records, confidences = [], [], []
    for row_number, raw_record in enumerate(outcome.records, start=1):
        standardized = standardize_record(
            raw_record, vendor_id=vendor_id, shop=shop,
            derive_amount_from_outstanding=derive_amount_from_outstanding,
        )
        standardized["page_number"] = page_number
        standardized["row_number"] = row_number

        confidence = standardized.get("extraction_confidence")
        if isinstance(confidence, (int, float)):
            confidences.append(float(confidence))

        result = validate_extraction(standardized, validation_rules)
        if result.is_valid:
            bronze_records.append(standardized)
        else:
            review_queue_records.append(build_review_queue_record(
                vendor_id=vendor_id, source_file=source_file, statement_id=statement_id,
                statement_period=statement_period, pipeline_stage="AI_EXTRACTION",
                rejection_category=result.category,
                rejection_details=result.details,
                extraction_confidence=result.checked_confidence,
                confidence_threshold_applied=result.confidence_threshold,
                raw_payload=json.dumps(raw_record, default=str),
            ))

    total = len(outcome.records)
    validation_result = f"{len(bronze_records)}/{total} valid" if total else "0/0 valid"
    # ai_audit_log's extraction_confidence is one value per call (per page here),
    # so a page with several invoice lines is summarized as their average --
    # not a substitute for the per-record extraction_confidence already on
    # each Bronze/review-queue row.
    average_confidence = sum(confidences) / len(confidences) if confidences else None
    audit_record = build_audit_record(
        outcome.ai_response, interaction_type="EXTRACTION", ai_provider=ai_provider,
        prompt_version=prompt_version, source_file=source_file, vendor_id=vendor_id,
        statement_id=statement_id, validation_result=validation_result,
        extraction_confidence=average_confidence,
    )
    for row in review_queue_records:
        row["ai_audit_id"] = audit_record["audit_id"]

    return PageExtractionResult(bronze_records=bronze_records, review_queue_records=review_queue_records, audit_record=audit_record)


def _failed_page_result(
    ai_response: AIResponse, *, rejection_category: str, rejection_details: str, validation_result: str,
    vendor_id: str, source_file: str, statement_id: Optional[str], statement_period: str,
    ai_provider: str, prompt_version: str,
) -> PageExtractionResult:
    audit_record = build_audit_record(
        ai_response, interaction_type="EXTRACTION", ai_provider=ai_provider, prompt_version=prompt_version,
        source_file=source_file, vendor_id=vendor_id, statement_id=statement_id, validation_result=validation_result,
    )
    review_row = build_review_queue_record(
        vendor_id=vendor_id, source_file=source_file, statement_id=statement_id,
        statement_period=statement_period, pipeline_stage="AI_EXTRACTION",
        rejection_category=rejection_category, rejection_details=rejection_details,
        raw_payload=ai_response.text or (ai_response.error or ""),
        ai_audit_id=audit_record["audit_id"],
    )
    return PageExtractionResult(bronze_records=[], review_queue_records=[review_row], audit_record=audit_record)


def deduplicate_batch(
    records: list,
    *,
    key_fields: list,
    vendor_id: str,
    source_file: str,
    statement_id: Optional[str],
    statement_period: str,
) -> tuple:
    """
    Runs once across a full document's already-standardized records (not
    per page -- duplicates can span pages). Returns (kept, review_queue_records)
    -- the first occurrence of each key is kept, later ones are flagged.
    """
    duplicate_indices = set(find_duplicate_indices(records, key_fields))
    kept = [r for i, r in enumerate(records) if i not in duplicate_indices]
    review_queue_records = [
        build_review_queue_record(
            vendor_id=vendor_id, source_file=source_file, statement_id=statement_id,
            statement_period=statement_period, pipeline_stage="VALIDATION",
            rejection_category="DUPLICATE_RECORD",
            rejection_details=f"Duplicate on {key_fields}: {[records[i].get(k) for k in key_fields]}",
            raw_payload=json.dumps(records[i], default=str),
        )
        for i in sorted(duplicate_indices)
    ]
    return kept, review_queue_records
