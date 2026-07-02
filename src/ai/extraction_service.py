"""
extraction_service.py

STATUS: Interface defined now. Implementation lands in the "Refactor PDF
Extraction" phase, together with the Validation Layer and the Bronze
schema extension it depends on.

Purpose (for that phase)
-------------------------
Wraps an AIClient with an extraction-specific prompt template and the
target JSON schema (Vendor, Invoice Number, Invoice Date, Amount,
Outstanding Amount, RO Number, PO Number, Due Date, Shop, Currency,
Statement Period). Returns a list of dicts ready for the Validation
Layer -- callers never see Gemini's raw response or prompt text.

Design commitment
------------------
extraction_service.py depends on AIClient (the abstract interface),
never on GeminiClient directly. 01_bronze_ingestion.py will depend on
extraction_service, never on src/ai/gemini_client.py directly. This is
the layering that makes the provider swappable later.
"""

from .base_client import AIClient


class ExtractionService:
    def __init__(self, ai_client: AIClient, vendor_config: dict):
        self.ai_client = ai_client
        self.vendor_config = vendor_config

    def extract_invoices_from_text(self, statement_text: str) -> list[dict]:
        """
        NOT YET IMPLEMENTED -- lands in the PDF extraction refactor phase.

        Will: build a prompt instructing Gemini to return a JSON array of
        invoice-line objects matching the target schema, call
        self.ai_client.generate(...), and return the parsed list (or raise
        a clear error for the Validation Layer to catch upstream of Bronze).
        """
        raise NotImplementedError("ExtractionService lands in the PDF extraction refactor phase.")
