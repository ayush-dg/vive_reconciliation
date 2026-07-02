"""
tests/test_audit_logger.py

Validates src/ai/audit_logger.py -- both the record shape it builds and
the failure classification logic that turns GeminiClient's free-text
error strings into a stable, dashboard-friendly status vocabulary.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ai.audit_logger import build_audit_record
from src.ai.base_client import AIResponse


def test_successful_response_builds_correct_record():
    response = AIResponse(success=True, model="gemini-2.0-flash", latency_ms=812.5, attempt_count=1)
    record = build_audit_record(
        response, interaction_type="EXTRACTION", ai_provider="gemini", prompt_version="extraction_v1",
        source_file="astech_vendor_statement_may2026.pdf", vendor_id="ASTECH",
        validation_result="PASSED", extraction_confidence=0.95,
    )
    assert record["success"] is True
    assert record["response_status"] == "SUCCESS"
    assert record["model"] == "gemini-2.0-flash"
    assert record["latency_ms"] == 812.5
    assert record["attempt_count"] == 1
    assert record["vendor_id"] == "ASTECH"
    assert record["extraction_confidence"] == 0.95
    assert record["validation_result"] == "PASSED"
    assert record["audit_id"]  # non-empty surrogate key generated


def test_prompt_text_is_never_stored_only_version():
    response = AIResponse(success=True, model="gemini-2.0-flash")
    record = build_audit_record(response, interaction_type="EXTRACTION", ai_provider="gemini", prompt_version="extraction_v1")
    assert "prompt" not in {k.lower() for k in record if "text" in k.lower()}
    assert record["prompt_version"] == "extraction_v1"


def test_missing_api_key_classified_correctly():
    response = AIResponse(success=False, error="Missing API key -- environment variable 'GEMINI_API_KEY' is not set.")
    record = build_audit_record(response, interaction_type="EXTRACTION", ai_provider="gemini", prompt_version="v1")
    assert record["response_status"] == "MISSING_API_KEY"


def test_transport_error_classified_correctly():
    response = AIResponse(success=False, error="Transport error on attempt 3: connection timed out")
    record = build_audit_record(response, interaction_type="EXTRACTION", ai_provider="gemini", prompt_version="v1")
    assert record["response_status"] == "TRANSPORT_ERROR"


def test_parse_error_classified_correctly():
    response = AIResponse(success=False, error="Response was not valid JSON despite responseMimeType=application/json: ...")
    record = build_audit_record(response, interaction_type="EXTRACTION", ai_provider="gemini", prompt_version="v1")
    assert record["response_status"] == "PARSE_ERROR"


def test_http_error_classified_correctly():
    response = AIResponse(success=False, error="HTTP 503 on attempt 3: {'error': 'unavailable'}")
    record = build_audit_record(response, interaction_type="EXTRACTION", ai_provider="gemini", prompt_version="v1")
    assert record["response_status"] == "HTTP_ERROR"


def test_unrecognized_error_falls_back_to_unknown():
    response = AIResponse(success=False, error="something completely unexpected happened")
    record = build_audit_record(response, interaction_type="EXTRACTION", ai_provider="gemini", prompt_version="v1")
    assert record["response_status"] == "UNKNOWN_ERROR"


def test_interaction_type_is_not_hardcoded_to_extraction():
    response = AIResponse(success=True, model="gemini-2.0-flash")
    record = build_audit_record(response, interaction_type="EXECUTIVE_SUMMARY", ai_provider="gemini", prompt_version="summary_v1")
    assert record["interaction_type"] == "EXECUTIVE_SUMMARY"


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
