"""
explanation_service.py

STATUS: Interface defined now. Implementation lands in the "AI for
Exception Analysis" phase, which runs strictly AFTER the deterministic
Spark Matching Engine has already decided what's matched vs. unmatched.

Purpose (for that phase)
-------------------------
Takes unmatched/exception rows (already classified by reason code via
deterministic rules -- see the Matching Engine) and asks Gemini for a
business-friendly probable reason, explanation, suggested resolution,
and confidence score. Writes into gold_exceptions' ai_* columns.

Design commitment
------------------
This service NEVER influences match_status or exception_reason -- those
are already final by the time this runs. It only adds human-readable
context on top of a decision Spark already made deterministically.
"""

from .base_client import AIClient


class ExplanationService:
    def __init__(self, ai_client: AIClient):
        self.ai_client = ai_client

    def explain_exception(self, exception_row: dict) -> dict:
        """
        NOT YET IMPLEMENTED -- lands in the AI Exception Analysis phase.

        Will return a dict with keys: probable_reason, explanation,
        suggested_resolution, confidence_score -- for exactly one
        already-classified exception row. Never called for matched rows.
        """
        raise NotImplementedError("ExplanationService lands in the AI Exception Analysis phase.")
