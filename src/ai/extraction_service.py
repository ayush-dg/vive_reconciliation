"""
extraction_service.py

Purpose
-------
Wraps an AIClient with an extraction-specific prompt template and parses its
response into a list of raw invoice-line dicts. The target field names come
straight from the vendor config's source_column_mapping -- nothing
vendor-specific is hardcoded into the prompt, so a new vendor config with a
different set of statement columns changes what this asks Gemini for without
any code change here.

What this does NOT do
----------------------
- Does not know about vendor_id or shop -- those are known context the
  caller already has (vendor_id from config, shop from a one-time regex
  extraction against the statement header), not something worth asking an
  LLM to infer per invoice line.
- Does not validate, standardize field names (e.g. confidence ->
  extraction_confidence), or derive missing fields (e.g. amount from
  outstanding_amount) -- that is pipeline-glue's job
  (src/ai/extraction_pipeline.py::standardize_record), so this stays a thin,
  reusable "ask the provider, get raw structured records back" wrapper.

Design commitment
------------------
extraction_service.py depends on AIClient (the abstract interface), never on
GeminiClient directly. 01_bronze_ingestion.py depends on this module, never
on src/ai/gemini_client.py directly. This is the layering that makes the
provider swappable later.
"""

from dataclasses import dataclass, field
from typing import Optional

from .base_client import AIClient, AIResponse


@dataclass
class ExtractionOutcome:
    """
    Result of one extract_invoices_from_text() call.

    ai_response : the raw AIClient response -- callers need this for audit
                  logging (latency, attempt_count, success, error), which
                  has nothing to do with whether any usable records came
                  back.
    records     : raw invoice-line dicts as returned by the provider, keyed
                  by the vendor config's source_column_mapping values, plus
                  a provider-supplied "confidence" key. Empty when the call
                  failed or the response didn't match the expected shape.
    error       : set when ai_response.success is True but the JSON didn't
                  match the {"invoices": [...]} contract this service asked
                  for -- distinct from a transport/HTTP-level failure, which
                  is already captured on ai_response.error.
    """
    ai_response: AIResponse
    records: list = field(default_factory=list)
    error: Optional[str] = None


class ExtractionService:
    PROMPT_VERSION = "extraction_v1"

    def __init__(self, ai_client: AIClient, vendor_config: dict):
        self.ai_client = ai_client
        self.vendor_config = vendor_config

    def extract_invoices_from_text(self, statement_text: str) -> ExtractionOutcome:
        """
        Sends one page (or document) of already-extracted statement text to
        the configured AIClient and parses its JSON response into raw
        invoice-line dicts.

        Returns an ExtractionOutcome in every case -- never raises on a
        provider failure or a malformed response, since a single bad page
        must not take down the whole ingestion run. Callers (see
        extraction_pipeline.process_page) are responsible for deciding what
        happens next: validation, review queue, fallback.
        """
        prompt = self._build_prompt(statement_text)
        response = self.ai_client.generate(prompt)

        if not response.success:
            return ExtractionOutcome(ai_response=response, records=[])

        invoices = response.parsed_json.get("invoices") if isinstance(response.parsed_json, dict) else None

        if not isinstance(invoices, list):
            return ExtractionOutcome(
                ai_response=response,
                records=[],
                error=(
                    "Expected a JSON object shaped like {'invoices': [...]}, "
                    f"got: {type(response.parsed_json).__name__}"
                ),
            )

        records = [item for item in invoices if isinstance(item, dict)]
        return ExtractionOutcome(ai_response=response, records=records)

    def _build_prompt(self, statement_text: str) -> str:
        mapping = self.vendor_config["source_column_mapping"]
        field_names = list(dict.fromkeys(mapping.values()))  # de-duped, order preserved
        vendor_name = self.vendor_config.get("vendor_name", self.vendor_config.get("vendor_id", "the vendor"))
        date_format = self.vendor_config.get("date_format", "exactly as printed")
        invoice_pattern = self.vendor_config.get("invoice_pattern")

        field_list = ", ".join(f'"{f}"' for f in field_names)
        schema_example = ", ".join(f'"{f}": "..."' for f in field_names)
        invoice_pattern_note = (
            f'Invoice numbers usually match the pattern {invoice_pattern} -- '
            "report them exactly as printed, including any revision suffix."
            if invoice_pattern else
            "Report invoice numbers exactly as printed, including any revision suffix."
        )

        return f"""You are extracting invoice line items from a vendor statement for {vendor_name}.

The text below was extracted from one page of the statement PDF. Each
invoice line has these fields: {field_list}.

Return ONLY a JSON object of this exact shape, with one entry per invoice
line item found on this page (an empty list if this page has none):

{{"invoices": [{{{schema_example}, "confidence": 0.0}}]}}

Rules:
- Preserve every date exactly as printed, in {date_format} format.
- Return the outstanding-amount field as a plain numeric string with no
  currency symbol and no thousands separator (e.g. "1234.56", not
  "$1,234.56").
- {invoice_pattern_note}
- "confidence" is your own 0.0-1.0 estimate of how clearly this specific
  line was legible and unambiguous in the source text.
- Do not invent a line item that isn't present in the text. Do not include
  header, footer, or total rows.

Statement page text:
---
{statement_text}
---
"""
