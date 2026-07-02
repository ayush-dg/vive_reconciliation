"""
summary_service.py

STATUS: Interface defined now. Implementation lands in the "AI Executive
Summary" phase, which runs after Gold tables (including
gold_reconciliation_summary) are populated for a given vendor/period.

Purpose (for that phase)
-------------------------
Reads gold_reconciliation_summary + gold_exceptions for a vendor/period
and asks Gemini to produce a short executive narrative: invoices
processed, match percentage, total exceptions, major exception
categories, recommendations, business observations. Written to its own
table so Power BI can display it without re-deriving anything.

Design commitment
------------------
Purely descriptive. Never recomputes or overrides any Gold metric --
only narrates numbers Spark already finalized.
"""

from .base_client import AIClient


class SummaryService:
    def __init__(self, ai_client: AIClient):
        self.ai_client = ai_client

    def generate_executive_summary(self, reconciliation_summary_row: dict, exception_rows: list[dict]) -> str:
        """
        NOT YET IMPLEMENTED -- lands in the AI Executive Summary phase.

        Will return a plain-language narrative string for one
        vendor/shop/period, built only from already-finalized Gold data.
        """
        raise NotImplementedError("SummaryService lands in the AI Executive Summary phase.")
