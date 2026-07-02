"""
base_client.py

Purpose
-------
This is the contract. Nothing outside src/ai/ should ever import
GeminiClient directly -- every service (extraction, explanation, summary)
depends on AIClient, this abstract interface, instead. That's what makes
"swap Gemini for Azure OpenAI later" a one-file change: write a new class
that implements AIClient, wire it up in place of GeminiClient, and every
downstream service keeps working unmodified.

AIResponse is intentionally provider-neutral -- it never exposes Gemini's
specific response shape (candidates[0].content.parts[0].text, etc.) to
the rest of the codebase. Each provider's client is responsible for
translating its own wire format into this shape.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class AIResponse:
    success: bool
    text: str = ""                     # raw text of the model's reply
    parsed_json: Optional[dict] = None  # populated when response_mime_type = "application/json" and parsing succeeded
    model: str = ""
    latency_ms: float = 0.0
    attempt_count: int = 1             # how many HTTP attempts this took, including retries
    error: Optional[str] = None        # human-readable failure reason when success = False
    raw_response: Any = field(default=None, repr=False)  # full provider response, kept for audit/lineage, never parsed by callers


class AIClient(ABC):
    """
    Abstract interface every AI provider adapter must implement.

    Deliberately minimal -- one method. Extraction, explanation, and
    summary services all just need "send this prompt, get structured
    text back." Anything provider-specific (auth headers, retry
    semantics, endpoint construction) is the concrete client's problem,
    not something callers of this interface ever see.
    """

    @abstractmethod
    def generate(
        self,
        prompt: str,
        *,
        temperature: Optional[float] = None,
        max_output_tokens: Optional[int] = None,
    ) -> AIResponse:
        """
        Send a prompt, get a structured response back.

        temperature / max_output_tokens are optional PER-CALL overrides
        of the config file's defaults -- e.g. extraction might want a
        lower temperature than an executive summary. Neither is ever
        hardcoded by a caller; both trace back to config/ai/gemini.json
        unless explicitly overridden.
        """
        raise NotImplementedError
