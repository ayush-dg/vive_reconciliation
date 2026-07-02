"""
audit_logger.py

Purpose
-------
Turns an AIResponse (from ANY AIClient implementation) plus call context
into one audit log row, ready to write to ai_audit_log. Used uniformly
by ExtractionService, ExplanationService, and SummaryService so audit
logging exists in exactly one place instead of being duplicated three
times with three chances to drift out of sync.

Writes plain dicts, never touches Spark or a DataFrame directly -- keeps
this framework-agnostic and trivially unit-testable without a
SparkSession. The calling notebook is responsible for turning a list of
these dicts into a Spark DataFrame and appending to ai_audit_log.

Explicitly NOT captured here: full prompt text. Only prompt_version, a
short version identifier (e.g. "extraction_v1") -- per the requirement
that this table records AI activity for observability, not a transcript
of every prompt sent.
"""

import uuid
from datetime import datetime, timezone
from typing import Optional

from .base_client import AIResponse


def build_audit_record(
    response: AIResponse,
    *,
    interaction_type: str,          # 'EXTRACTION' | 'EXCEPTION_EXPLANATION' | 'EXECUTIVE_SUMMARY'
    ai_provider: str,               # e.g. 'gemini' -- not read from the response, since AIResponse is provider-neutral
    prompt_version: str,            # e.g. 'extraction_v1' -- never the full prompt text
    source_file: Optional[str] = None,
    vendor_id: Optional[str] = None,
    statement_id: Optional[str] = None,
    validation_result: Optional[str] = None,
    extraction_confidence: Optional[float] = None,
) -> dict:
    return {
        "audit_id": str(uuid.uuid4()),
        "source_file": source_file,
        "vendor_id": vendor_id,
        "statement_id": statement_id,
        "interaction_type": interaction_type,
        "ai_provider": ai_provider,
        "model": response.model,
        "prompt_version": prompt_version,
        "request_timestamp": datetime.now(timezone.utc),
        "latency_ms": response.latency_ms,
        "attempt_count": response.attempt_count,
        "success": response.success,
        "response_status": "SUCCESS" if response.success else _classify_failure(response.error),
        "error_message": response.error,
        "extraction_confidence": extraction_confidence,
        "validation_result": validation_result,
    }


def _classify_failure(error_message: Optional[str]) -> str:
    """
    Buckets GeminiClient's free-text error strings into a small, stable
    set of status codes -- so a dashboard querying ai_audit_log can
    group by response_status without parsing error text itself. If a
    future provider adapter raises different error phrasing, add a
    branch here; callers of build_audit_record never need to change.
    """
    if not error_message:
        return "UNKNOWN_ERROR"
    msg = error_message.lower()
    if "missing api key" in msg or "environment variable" in msg:
        return "MISSING_API_KEY"
    if "transport error" in msg:
        return "TRANSPORT_ERROR"
    if "not valid json" in msg:
        return "PARSE_ERROR"
    if "unexpected response shape" in msg:
        return "PARSE_ERROR"
    if msg.startswith("http "):
        return "HTTP_ERROR"
    return "UNKNOWN_ERROR"
