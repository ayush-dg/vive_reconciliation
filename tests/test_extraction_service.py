"""
tests/test_extraction_service.py

Validates src/ai/extraction_service.py entirely through a fake AIClient --
no network access, no real Gemini call, matching the style of
tests/test_gemini_client.py (which fakes the transport instead of the
client). Here we fake the client itself since ExtractionService depends on
the AIClient interface, not on GeminiClient.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ai.base_client import AIClient, AIResponse
from src.ai.extraction_service import ExtractionService

VENDOR_CONFIG = {
    "vendor_id": "ASTECH",
    "vendor_name": "Repairify, Inc dba asTech",
    "source_column_mapping": {
        "Invoice Date": "invoice_date",
        "Invoice #": "invoice_number",
        "Work Order #": "work_order_number",
        "RO #": "ro_number",
        "Outstanding Amount": "outstanding_amount",
        "Due Date": "due_date",
    },
    "date_format": "MM/dd/yyyy",
    "invoice_pattern": "^SIN\\d+$",
}


class FakeAIClient(AIClient):
    """Returns a pre-baked AIResponse, capturing the prompt it was called with."""

    def __init__(self, response: AIResponse):
        self.response = response
        self.last_prompt = None

    def generate(self, prompt, *, temperature=None, max_output_tokens=None):
        self.last_prompt = prompt
        return self.response


def _success_response(parsed_json):
    return AIResponse(success=True, text="irrelevant", parsed_json=parsed_json, model="fake-model", attempt_count=1)


def test_successful_extraction_returns_raw_records():
    parsed = {"invoices": [
        {"invoice_date": "05/01/2026", "invoice_number": "SIN12200241", "work_order_number": "WO1",
         "ro_number": "RO1", "outstanding_amount": "48.75", "due_date": "05/31/2026", "confidence": 0.95},
    ]}
    client = FakeAIClient(_success_response(parsed))
    service = ExtractionService(client, VENDOR_CONFIG)

    outcome = service.extract_invoices_from_text("some statement text")

    assert outcome.ai_response.success is True
    assert outcome.error is None
    assert len(outcome.records) == 1
    assert outcome.records[0]["invoice_number"] == "SIN12200241"
    # Renaming confidence -> extraction_confidence is pipeline-glue's job,
    # not this service's -- the raw provider key must survive untouched here.
    assert outcome.records[0]["confidence"] == 0.95
    assert "extraction_confidence" not in outcome.records[0]


def test_empty_invoice_list_is_valid():
    client = FakeAIClient(_success_response({"invoices": []}))
    service = ExtractionService(client, VENDOR_CONFIG)

    outcome = service.extract_invoices_from_text("a page with no invoices")

    assert outcome.ai_response.success is True
    assert outcome.error is None
    assert outcome.records == []


def test_ai_call_failure_yields_no_records_and_propagates_response():
    failed_response = AIResponse(success=False, error="HTTP 503 on attempt 3: {'error': 'unavailable'}", attempt_count=3)
    client = FakeAIClient(failed_response)
    service = ExtractionService(client, VENDOR_CONFIG)

    outcome = service.extract_invoices_from_text("some statement text")

    assert outcome.ai_response.success is False
    assert outcome.records == []
    assert outcome.error is None  # the failure is on ai_response, not a shape-contract error


def test_missing_invoices_key_is_a_contract_error():
    client = FakeAIClient(_success_response({"unexpected": "shape"}))
    service = ExtractionService(client, VENDOR_CONFIG)

    outcome = service.extract_invoices_from_text("some statement text")

    assert outcome.ai_response.success is True
    assert outcome.records == []
    assert outcome.error is not None
    assert "invoices" in outcome.error


def test_non_list_invoices_value_is_a_contract_error():
    client = FakeAIClient(_success_response({"invoices": "not a list"}))
    service = ExtractionService(client, VENDOR_CONFIG)

    outcome = service.extract_invoices_from_text("some statement text")

    assert outcome.records == []
    assert outcome.error is not None


def test_non_dict_items_in_invoices_list_are_dropped():
    client = FakeAIClient(_success_response({"invoices": [{"invoice_number": "SIN1"}, "garbage", 42]}))
    service = ExtractionService(client, VENDOR_CONFIG)

    outcome = service.extract_invoices_from_text("some statement text")

    assert len(outcome.records) == 1
    assert outcome.records[0]["invoice_number"] == "SIN1"


def test_prompt_is_driven_by_vendor_config_not_hardcoded():
    client = FakeAIClient(_success_response({"invoices": []}))
    service = ExtractionService(client, VENDOR_CONFIG)

    service.extract_invoices_from_text("PAGE TEXT MARKER")

    prompt = client.last_prompt
    assert "PAGE TEXT MARKER" in prompt
    for field_name in VENDOR_CONFIG["source_column_mapping"].values():
        assert field_name in prompt
    assert VENDOR_CONFIG["date_format"] in prompt
    assert VENDOR_CONFIG["invoice_pattern"] in prompt


def test_prompt_works_without_optional_config_fields():
    minimal_config = {
        "vendor_id": "QUIRK",
        "source_column_mapping": {"Invoice #": "invoice_number", "Amount": "outstanding_amount"},
    }
    client = FakeAIClient(_success_response({"invoices": []}))
    service = ExtractionService(client, minimal_config)

    outcome = service.extract_invoices_from_text("text")

    assert outcome.error is None
    assert "invoice_number" in client.last_prompt


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
