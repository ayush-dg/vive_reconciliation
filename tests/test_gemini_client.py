"""
tests/test_gemini_client.py

Validates src/ai/gemini_client.py entirely through an injected fake
transport -- no network access, no real API key needed. This is
deliberate: an AI service layer's retry/error/parsing logic should be
testable independent of whether the provider's endpoint is reachable
from wherever tests happen to run.
"""

import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ai.gemini_client import GeminiClient

BASE_CONFIG = {
    "model": "gemini-2.0-flash",
    "api_key_env_var": "TEST_GEMINI_API_KEY",
    "endpoint_template": "https://fake.endpoint/{model}?key={api_key}",
    "temperature": 0.1,
    "max_output_tokens": 2048,
    "timeout_seconds": 5,
    "response_mime_type": "application/json",
    "retry_policy": {
        "max_retries": 2,
        "backoff_seconds": 0,       # zero so tests run instantly
        "backoff_multiplier": 1,
        "retry_on_status_codes": [429, 500, 502, 503, 504],
    },
}


def _gemini_success_body(text: str):
    return {"candidates": [{"content": {"parts": [{"text": text}]}}]}


def setup_api_key():
    os.environ["TEST_GEMINI_API_KEY"] = "fake-key-for-tests"


def test_successful_call_parses_json():
    setup_api_key()
    calls = []

    def fake_transport(url, headers, body, timeout):
        calls.append((url, headers, body, timeout))
        return 200, _gemini_success_body('{"invoice_number": "SIN12200241", "amount": 48.75}')

    client = GeminiClient(BASE_CONFIG, transport=fake_transport)
    response = client.generate("extract this invoice")

    assert response.success is True
    assert response.parsed_json == {"invoice_number": "SIN12200241", "amount": 48.75}
    assert response.attempt_count == 1
    assert len(calls) == 1


def test_config_values_are_passed_into_request_not_hardcoded():
    setup_api_key()
    captured = {}

    def fake_transport(url, headers, body, timeout):
        captured["url"] = url
        captured["body"] = body
        captured["timeout"] = timeout
        return 200, _gemini_success_body("{}")

    client = GeminiClient(BASE_CONFIG, transport=fake_transport)
    client.generate("prompt")

    assert "gemini-2.0-flash" in captured["url"]
    assert captured["body"]["generationConfig"]["temperature"] == 0.1
    assert captured["body"]["generationConfig"]["maxOutputTokens"] == 2048
    assert captured["timeout"] == 5


def test_per_call_override_beats_config_default():
    setup_api_key()
    captured = {}

    def fake_transport(url, headers, body, timeout):
        captured["body"] = body
        return 200, _gemini_success_body("{}")

    client = GeminiClient(BASE_CONFIG, transport=fake_transport)
    client.generate("prompt", temperature=0.9)

    assert captured["body"]["generationConfig"]["temperature"] == 0.9


def test_missing_api_key_fails_cleanly_without_raising():
    os.environ.pop("MISSING_KEY_VAR", None)
    config = dict(BASE_CONFIG, api_key_env_var="MISSING_KEY_VAR")
    client = GeminiClient(config, transport=lambda *a: (200, {}))

    response = client.generate("prompt")

    assert response.success is False
    assert "MISSING_KEY_VAR" in response.error


def test_retries_on_transient_status_then_succeeds():
    setup_api_key()
    attempts = {"count": 0}

    def flaky_transport(url, headers, body, timeout):
        attempts["count"] += 1
        if attempts["count"] < 3:
            return 503, {"error": "temporarily unavailable"}
        return 200, _gemini_success_body('{"ok": true}')

    client = GeminiClient(BASE_CONFIG, transport=flaky_transport)
    response = client.generate("prompt")

    assert response.success is True
    assert response.attempt_count == 3
    assert attempts["count"] == 3


def test_exhausts_retries_and_fails_without_raising():
    setup_api_key()
    attempts = {"count": 0}

    def always_failing_transport(url, headers, body, timeout):
        attempts["count"] += 1
        return 500, {"error": "server error"}

    client = GeminiClient(BASE_CONFIG, transport=always_failing_transport)
    response = client.generate("prompt")

    assert response.success is False
    # max_retries = 2 -> 3 total attempts (1 initial + 2 retries)
    assert attempts["count"] == 3
    assert response.attempt_count == 3


def test_non_retryable_status_fails_fast_without_retrying():
    setup_api_key()
    attempts = {"count": 0}

    def bad_request_transport(url, headers, body, timeout):
        attempts["count"] += 1
        return 400, {"error": "bad request -- malformed prompt"}

    client = GeminiClient(BASE_CONFIG, transport=bad_request_transport)
    response = client.generate("prompt")

    assert response.success is False
    assert attempts["count"] == 1  # no retry -- 400 is not in retry_on_status_codes


def test_malformed_response_shape_fails_cleanly():
    setup_api_key()

    def malformed_transport(url, headers, body, timeout):
        return 200, {"unexpected": "shape"}

    client = GeminiClient(BASE_CONFIG, transport=malformed_transport)
    response = client.generate("prompt")

    assert response.success is False
    assert "Unexpected response shape" in response.error


def test_invalid_json_in_text_fails_cleanly():
    setup_api_key()

    def bad_json_transport(url, headers, body, timeout):
        return 200, _gemini_success_body("this is not valid json {{{")

    client = GeminiClient(BASE_CONFIG, transport=bad_json_transport)
    response = client.generate("prompt")

    assert response.success is False
    assert "not valid JSON" in response.error


def test_transport_exception_is_caught_and_retried():
    setup_api_key()
    attempts = {"count": 0}

    def flaky_network(url, headers, body, timeout):
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise TimeoutError("connection timed out")
        return 200, _gemini_success_body('{"ok": true}')

    client = GeminiClient(BASE_CONFIG, transport=flaky_network)
    response = client.generate("prompt")

    assert response.success is True
    assert attempts["count"] == 2


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
