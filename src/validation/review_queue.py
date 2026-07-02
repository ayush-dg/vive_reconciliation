"""
review_queue.py

Purpose
-------
Turns a rejected record (plus rejection context) into one
validation_document_review_queue row, ready to write to that Delta table.
Same role for the review queue that src/ai/audit_logger.py::build_audit_record
plays for ai_audit_log: one place that builds the row shape, so every
rejection path (AI extraction, future Silver-stage business rules) produces
a record with exactly the same shape instead of drifting out of sync.

Writes a plain dict, never touches Spark or a DataFrame directly -- the
calling notebook turns a list of these into a Spark DataFrame and appends to
validation_document_review_queue.
"""

import uuid
from datetime import datetime, timezone
from typing import Optional


def build_review_queue_record(
    *,
    vendor_id: str,
    source_file: str,
    statement_id: Optional[str],
    statement_period: str,
    pipeline_stage: str,             # 'AI_EXTRACTION' | 'VALIDATION' | 'SILVER_NORMALIZATION' (future)
    rejection_category: str,         # e.g. 'MISSING_MANDATORY_FIELD', 'LOW_CONFIDENCE', 'AI_CALL_FAILED'
    rejection_details: str,
    raw_payload: str,
    extraction_confidence: Optional[float] = None,
    confidence_threshold_applied: Optional[float] = None,
    ai_audit_id: Optional[str] = None,
) -> dict:
    return {
        "review_id": str(uuid.uuid4()),
        "vendor_id": vendor_id,
        "source_file": source_file,
        "statement_id": statement_id,
        "statement_period": statement_period,
        "pipeline_stage": pipeline_stage,
        "rejection_category": rejection_category,
        "rejection_details": rejection_details,
        "extraction_confidence": extraction_confidence,
        "confidence_threshold_applied": confidence_threshold_applied,
        "raw_payload": raw_payload,
        "ai_audit_id": ai_audit_id,
        "review_status": "PENDING_REVIEW",
        "flagged_timestamp": datetime.now(timezone.utc),
        "reviewed_by": None,
        "reviewed_timestamp": None,
        "resolution_notes": None,
    }
