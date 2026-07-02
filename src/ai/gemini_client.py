"""
gemini_client.py

Purpose
-------
The only file in this project that knows Gemini's specific wire format
(endpoint shape, request/response JSON structure, auth-via-query-param).
Everything else talks to AIClient, not this.

Design notes
------------
- Injectable transport: `_transport` defaults to a real HTTP implementation
  (used in Fabric, where the Gemini endpoint is reachable), but can be
  swapped for a fake in tests. This is what lets tests/test_gemini_client.py
  exercise retry logic, error handling, and request-building without ever
  making a network call.
- Every parameter that shapes a request (model, temperature, max tokens,
  timeout, retry policy) comes from the config dict passed into the
  constructor -- nothing is hardcoded here. See config/ai/gemini.json.
- The API key is read from an environment variable (name is itself
  config-driven via api_key_env_var) -- never stored in config, never
  hardcoded. In Fabric, this would be backed by a Fabric/Key Vault secret
  exposed as an environment variable to the notebook session.
- Retries only happen on the status codes listed in the config's
  retry_policy.retry_on_status_codes -- a 400 (bad request) fails fast
  and correctly; a 503 (transient) retries with backoff.
"""

import json
import os
import time
import urllib.request
import urllib.error
from typing import Callable, Optional

from .base_client import AIClient, AIResponse


class GeminiClient(AIClient):
    def __init__(self, config: dict, transport: Optional[Callable] = None):
        """
        config    : the parsed contents of config/ai/gemini.json
        transport : optional injectable callable with signature
                    (url, headers, body_dict, timeout_seconds) -> (status_code, response_dict)
                    Defaults to a real urllib-based HTTP call. Tests pass
                    a fake transport instead of touching the network.
        """
        self.config = config
        self._transport = transport or self._http_transport

        api_key_env_var = config.get("api_key_env_var", "GEMINI_API_KEY")
        self.api_key = os.environ.get(api_key_env_var)
        # Deliberately not raised here -- a missing key should only fail
        # the specific call that needs it, not block constructing the
        # client (e.g. for tests that inject a fake transport and never
        # touch the real key).

    def generate(self, prompt: str, *, temperature: Optional[float] = None, max_output_tokens: Optional[int] = None) -> AIResponse:
        model = self.config["model"]
        temperature = temperature if temperature is not None else self.config.get("temperature", 0.1)
        max_output_tokens = max_output_tokens if max_output_tokens is not None else self.config.get("max_output_tokens", 2048)
        timeout = self.config.get("timeout_seconds", 30)
        retry_policy = self.config.get("retry_policy", {"max_retries": 0, "backoff_seconds": 0, "backoff_multiplier": 1, "retry_on_status_codes": []})

        if not self.api_key:
            return AIResponse(success=False, model=model, error=f"Missing API key -- environment variable '{self.config.get('api_key_env_var')}' is not set.")

        url = self.config["endpoint_template"].format(model=model, api_key=self.api_key)
        headers = {"Content-Type": "application/json"}
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_output_tokens,
                "responseMimeType": self.config.get("response_mime_type", "text/plain"),
            },
        }

        max_retries = retry_policy.get("max_retries", 0)
        backoff = retry_policy.get("backoff_seconds", 0)
        multiplier = retry_policy.get("backoff_multiplier", 1)
        retry_statuses = set(retry_policy.get("retry_on_status_codes", []))

        attempt = 0
        last_error = None
        start = time.monotonic()

        while attempt <= max_retries:
            attempt += 1
            try:
                status_code, response_json = self._transport(url, headers, body, timeout)
            except Exception as e:
                # Transport-level failure (timeout, DNS, connection refused).
                # Treated the same as a retryable status -- retry if attempts remain.
                last_error = f"Transport error on attempt {attempt}: {e}"
                if attempt <= max_retries:
                    time.sleep(backoff * (multiplier ** (attempt - 1)))
                    continue
                break

            if status_code == 200:
                latency_ms = (time.monotonic() - start) * 1000
                return self._parse_success(response_json, model, latency_ms, attempt)

            last_error = f"HTTP {status_code} on attempt {attempt}: {response_json}"
            if status_code in retry_statuses and attempt <= max_retries:
                time.sleep(backoff * (multiplier ** (attempt - 1)))
                continue
            break

        return AIResponse(success=False, model=model, attempt_count=attempt, error=last_error)

    def _parse_success(self, response_json: dict, model: str, latency_ms: float, attempt: int) -> AIResponse:
        try:
            text = response_json["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError, TypeError) as e:
            return AIResponse(
                success=False, model=model, attempt_count=attempt, raw_response=response_json,
                error=f"Unexpected response shape from Gemini -- could not locate text: {e}",
            )

        parsed_json = None
        if self.config.get("response_mime_type") == "application/json":
            try:
                parsed_json = json.loads(text)
            except json.JSONDecodeError as e:
                return AIResponse(
                    success=False, model=model, attempt_count=attempt, text=text, raw_response=response_json,
                    error=f"Response was not valid JSON despite responseMimeType=application/json: {e}",
                )

        return AIResponse(
            success=True, text=text, parsed_json=parsed_json, model=model,
            latency_ms=latency_ms, attempt_count=attempt, raw_response=response_json,
        )

    @staticmethod
    def _http_transport(url: str, headers: dict, body: dict, timeout: int):
        """
        Real HTTP transport, used whenever this runs in Fabric (or anywhere
        with network access to Gemini's endpoint). Not exercised by unit
        tests -- those inject a fake transport instead.
        """
        data = json.dumps(body).encode("utf-8")
        request = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            try:
                error_json = json.loads(error_body)
            except json.JSONDecodeError:
                error_json = {"raw_error_body": error_body}
            return e.code, error_json
