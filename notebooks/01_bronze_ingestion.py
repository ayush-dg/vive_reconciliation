# ==========================================================================
# Notebook: 01_bronze_ingestion
# Purpose : Reads the asTech Vendor Statement PDF, extracts invoice-level
#           data, and lands it in bronze_vendor_statement_raw.
#
#           The Internal ERP side of Bronze (bronze_internal_erp_raw) is
#           NOT populated here anymore. Per the current architecture
#           (config/erp/internal_erp.json, active_adapter =
#           "mock_erp_generator"), that table is populated by
#           03_mock_erp_generator.py, seeded from Silver VENDOR_STATEMENT
#           rows -- not extracted from a Payment Voucher PDF. The
#           Payment Voucher extraction function is kept in this notebook,
#           fully intact, gated behind an adapter check -- see Cell 8.
#
# Phase B changes (AI Document Extraction):
#   - Gemini (via src/ai/extraction_service.py -> ExtractionService) is now
#     the PRIMARY extraction path for the vendor statement, driven by
#     config/vendors/astech.json's "extraction" block. Every AI-extracted
#     record passes through the existing validation gate
#     (src/validation/extraction_validator.py) via the new pipeline glue
#     (src/ai/extraction_pipeline.py): valid records go to Bronze, invalid
#     ones to validation_document_review_queue, and every AI call is logged
#     to ai_audit_log.
#   - The Phase A pdfplumber table-extraction path is retained, unchanged
#     in behavior, reachable two ways: (1) as a whole-document override via
#     extraction.active_method = "pdfplumber_tabular" in the vendor config,
#     reproducing Phase A exactly (no validation gate, no review queue, no
#     audit log -- this path was never designed to need one); (2) as a
#     reactive, per-page safety net when extraction.active_method is
#     "ai_extraction" but a specific page's AI call fails outright (see
#     extract_vendor_statement_via_ai). Partial per-row validation failures
#     (e.g. one low-confidence line on an otherwise-fine page) do NOT
#     trigger this fallback -- see extraction_pipeline.process_page's
#     docstring for why that's a deliberate scope boundary.
#   - The pdfplumber table-parsing logic itself was refactored into
#     _extract_page_table_rows() (one page at a time, returning
#     standard-schema dicts) so both the legacy whole-document path and the
#     new reactive per-page fallback share one implementation instead of
#     two copies of the same regex/column logic.
#
# Design notes (Phase A, unchanged):
#   - Extraction strategy is chosen per format, driven by config:
#       vendor config's "statement_format": "tabular_pdf" -> table-based
#         extraction (works because asTech's statement has real grid
#         lines pdfplumber can detect).
#   - Every row carries page_number and row_number for extraction-level
#     lineage, plus source_file / statement_id / ingestion_timestamp.
# ==========================================================================

# ---- CELL 1: Environment setup (Fabric-safe) ----------------------------
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("VIVE_Reconciliation_PoC_Local")
        .config("spark.sql.warehouse.dir", "/home/claude/vive_reconciliation_poc/lakehouse")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

import json
import re
import sys
import pdfplumber
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pyspark.sql import Row

sys.path.insert(0, ".")
from src.ai.gemini_client import GeminiClient
from src.ai.extraction_service import ExtractionService
from src.ai.extraction_pipeline import standardize_record, process_page, deduplicate_batch

# ---- CELL 2: Load config -- nothing vendor-specific is hardcoded below --
# In Fabric, config files live in the Lakehouse's Files/ area; locally
# they're read straight from the project's config/ folder. Either way,
# every extraction decision below traces back to one of these config files.
VENDOR_CONFIG_PATH = "config/vendors/astech.json"
ERP_CONFIG_PATH = "config/erp/internal_erp.json"
AI_CONFIG_PATH = "config/ai/gemini.json"
VALIDATION_RULES_PATH = "config/validation/extraction_rules.json"

with open(VENDOR_CONFIG_PATH) as f:
    vendor_config = json.load(f)

with open(ERP_CONFIG_PATH) as f:
    erp_config = json.load(f)

with open(AI_CONFIG_PATH) as f:
    gemini_config = json.load(f)

with open(VALIDATION_RULES_PATH) as f:
    validation_rules = json.load(f)

active_adapter_name = erp_config["active_adapter"]                 # "mock_erp_generator" for this PoC
erp_adapter = erp_config["adapters"][active_adapter_name]

# extraction.active_method picks AI vs. the legacy pdfplumber path for the
# vendor statement side. Defaults preserve Phase A behavior if a vendor
# config predates this block (e.g. a future vendor added before its AI
# extraction is ready).
extraction_config = vendor_config.get("extraction", {
    "active_method": "pdfplumber_tabular",
    "fallback_method": "pdfplumber_tabular",
    "fallback_on_ai_failure": False,
    "derive_amount_from_outstanding": False,
})

print(f"Vendor: {vendor_config['vendor_name']} ({vendor_config['vendor_id']})")
print(f"Active ERP adapter: {active_adapter_name} -> document_type = {erp_adapter['document_type']}")
print(f"Vendor statement extraction method: {extraction_config['active_method']}")

# ---- CELL 3: Shared per-page pdfplumber helpers -------------------------
# Used by BOTH the legacy whole-document extraction (Cell 4) and the AI
# path's reactive per-page fallback (Cell 5), so the table-parsing logic
# exists exactly once regardless of which path invokes it.

def _extract_shop_name(pdf, vendor_config: dict):
    """Shop name lives in the header text of page 1, not in the table."""
    shop_pattern = re.compile(vendor_config["shop_extraction"]["pattern"])
    header_text = pdf.pages[0].extract_text()
    match = shop_pattern.search(header_text)
    return match.group(1).strip() if match else None


def _extract_page_table_rows(page, page_number: int, shop_name, vendor_config: dict) -> list:
    """
    Table-based extraction for one page, for vendors whose statement_format
    is "tabular_pdf" AND whose PDF has detectable row gridlines (asTech
    qualifies -- see Phase 2 validation notes for vendors that don't, like
    Quirk, which will need a different extractor registered against a
    different statement_format value later).

    Returns standard-schema dicts (field names taken from the vendor
    config's source_column_mapping, in table-column order) rather than
    Bronze Rows directly -- that's what lets this same function feed the
    legacy whole-document path and the AI path's reactive fallback without
    duplicating this parsing logic.
    """
    field_names = list(vendor_config["source_column_mapping"].values())
    table = page.extract_table()
    if not table:
        return []

    rows_out = []
    for row_number, row in enumerate(table, start=1):
        # Skip the header row (only present on page 1) and the
        # "Total Outstanding Invoices:" footer row (last page).
        if row[0] in (None, "", "Invoice Date"):
            continue
        if row[0] and str(row[0]).startswith("Total"):
            continue
        if not row[1]:  # no invoice number -> not a real data row
            continue

        record = dict(zip(field_names, row))
        record["shop"] = shop_name
        record["page_number"] = page_number
        record["row_number"] = row_number
        rows_out.append(record)
    return rows_out


def _to_decimal(value, places: str = "0.0001"):
    """Bronze/audit/review-queue confidence columns are DECIMAL -- Spark
    schema-conformant conversion from whatever numeric type Python has."""
    if value is None:
        return None
    try:
        return Decimal(str(value)).quantize(Decimal(places))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _standard_record_to_bronze_row(
    record: dict, *, vendor_id, source_file, statement_id, statement_period, ingestion_ts,
    extraction_confidence=None, extraction_model=None, document_section=None,
    bounding_box=None, raw_gemini_response=None,
) -> Row:
    return Row(
        vendor_id=vendor_id,
        source_file=source_file,
        statement_id=statement_id,
        statement_period=statement_period,
        page_number=int(record["page_number"]) if record.get("page_number") is not None else None,
        row_number=int(record["row_number"]) if record.get("row_number") is not None else None,
        ingestion_timestamp=ingestion_ts,
        raw_invoice_date=record.get("invoice_date"),
        raw_invoice_number=record.get("invoice_number"),
        raw_work_order_number=record.get("work_order_number"),
        raw_ro_number=record.get("ro_number"),
        raw_outstanding_amount=record.get("outstanding_amount"),
        raw_due_date=record.get("due_date"),
        raw_shop_name=record.get("shop"),
        extraction_confidence=extraction_confidence,
        extraction_model=extraction_model,
        document_section=document_section,
        bounding_box=bounding_box,
        raw_gemini_response=raw_gemini_response,
    )


# ---- CELL 4: Legacy whole-document pdfplumber extraction (Phase A) ------
def extract_vendor_statement(pdf_path: str, vendor_id: str, vendor_config: dict, statement_id: str, statement_period: str, source_file: str):
    """
    Whole-document pdfplumber table extraction -- unchanged Phase A
    behavior. Reachable via extraction.active_method = "pdfplumber_tabular"
    in config/vendors/astech.json. No validation gate, no review queue, no
    audit log: this path was never designed to need a confidence gate, and
    Phase B deliberately leaves that behavior exactly as it was.
    """
    ingestion_ts = datetime.now(timezone.utc)
    rows_out = []

    with pdfplumber.open(pdf_path) as pdf:
        shop_name = _extract_shop_name(pdf, vendor_config)
        for page_number, page in enumerate(pdf.pages, start=1):
            for record in _extract_page_table_rows(page, page_number, shop_name, vendor_config):
                rows_out.append(_standard_record_to_bronze_row(
                    record, vendor_id=vendor_id, source_file=source_file, statement_id=statement_id,
                    statement_period=statement_period, ingestion_ts=ingestion_ts,
                ))
    return rows_out


# ---- CELL 5: AI-first extraction path (Phase B) --------------------------
def extract_vendor_statement_via_ai(
    pdf_path: str, vendor_id: str, vendor_config: dict, statement_id: str, statement_period: str, source_file: str,
    extraction_service: ExtractionService, validation_rules: dict, extraction_config: dict,
):
    """
    Gemini (via extraction_service) structures each page's text; every
    returned record passes through the same validation gate any future
    provider would (src/validation/extraction_validator.py, via
    extraction_pipeline.process_page). Failures go to
    validation_document_review_queue; every AI call is logged to
    ai_audit_log regardless of outcome.

    If a page's AI call fails outright (total failure -- an API/transport
    error, or a response that didn't match the expected JSON contract; see
    process_page's docstring) and extraction_config["fallback_on_ai_failure"]
    is true, that page is re-extracted with the legacy pdfplumber table
    parser as a reactive, per-page safety net so a transient AI failure
    doesn't lose real invoice data. Partial validation failures on an
    otherwise-successful page are NOT auto-recovered this way -- they are
    genuinely new data for human review, not a parsing failure.

    Deduplication runs once across the whole document's collected records
    (not per page), since a duplicate can span pages.
    """
    ingestion_ts = datetime.now(timezone.utc)
    derive_amount = extraction_config["derive_amount_from_outstanding"]
    standard_records = []   # merged AI + reactive-fallback records, pre-dedup
    review_queue_rows = []
    audit_rows = []

    with pdfplumber.open(pdf_path) as pdf:
        shop_name = _extract_shop_name(pdf, vendor_config)

        for page_number, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            outcome = extraction_service.extract_invoices_from_text(text)
            result = process_page(
                outcome, validation_rules=validation_rules, vendor_id=vendor_id, shop=shop_name,
                source_file=source_file, statement_id=statement_id, statement_period=statement_period,
                page_number=page_number, derive_amount_from_outstanding=derive_amount,
            )
            audit_rows.append(result.audit_record)
            review_queue_rows.extend(result.review_queue_records)

            if result.bronze_records:
                for record in result.bronze_records:
                    record["_extraction_model"] = gemini_config["model"]
                    record["_raw_gemini_response"] = outcome.ai_response.text
                standard_records.extend(result.bronze_records)
            elif extraction_config.get("fallback_on_ai_failure"):
                print(f"  Page {page_number}: AI extraction failed outright -- falling back to pdfplumber for this page.")
                fallback_records = [
                    standardize_record(r, vendor_id=vendor_id, shop=shop_name, derive_amount_from_outstanding=derive_amount)
                    for r in _extract_page_table_rows(page, page_number, shop_name, vendor_config)
                ]
                for record in fallback_records:
                    record["_extraction_model"] = None
                    record["_raw_gemini_response"] = None
                standard_records.extend(fallback_records)

    key_fields = validation_rules.get("duplicate_key_fields", ["vendor", "invoice_number", "amount"])
    kept_records, dup_review_rows = deduplicate_batch(
        standard_records, key_fields=key_fields, vendor_id=vendor_id, source_file=source_file,
        statement_id=statement_id, statement_period=statement_period,
    )
    review_queue_rows.extend(dup_review_rows)

    bronze_rows = [
        _standard_record_to_bronze_row(
            record, vendor_id=vendor_id, source_file=source_file, statement_id=statement_id,
            statement_period=statement_period, ingestion_ts=ingestion_ts,
            extraction_confidence=_to_decimal(record.get("extraction_confidence")),
            extraction_model=record.get("_extraction_model"),
            document_section=record.get("document_section"),
            raw_gemini_response=record.get("_raw_gemini_response"),
        )
        for record in kept_records
    ]
    return bronze_rows, review_queue_rows, audit_rows


# ---- CELL 6: Dispatch -- run the configured extraction method -----------
# Same environment-detection spirit as Cell 1's `try: spark` -- lets
# scripts/run_pipeline.py's Demo Mode point this notebook at a caller-
# supplied PDF (and, optionally, a different statement_id/period) by
# pre-binding any of these names in the shared exec namespace before this
# notebook runs. Each name defaults independently (via globals()
# membership, not try/except) so a partial override -- e.g. a different
# PDF with the default statement_id -- works as expected. A normal/
# default run never pre-sets any of them, so every one falls through to
# the exact same hardcoded sample-PDF values as before.
if "STATEMENT_ID" not in globals():
    STATEMENT_ID = "ASTECH-COLLEX-2026-05"
if "STATEMENT_PERIOD" not in globals():
    STATEMENT_PERIOD = "2026-05"
if "SOURCE_FILE" not in globals():
    SOURCE_FILE = "astech_vendor_statement_may2026.pdf"
if "PDF_PATH" not in globals():
    PDF_PATH = f"sample_data/{SOURCE_FILE}"

if extraction_config["active_method"] == "ai_extraction":
    ai_client = GeminiClient(gemini_config)
    extraction_service = ExtractionService(ai_client, vendor_config)
    statement_rows, review_queue_rows, audit_rows = extract_vendor_statement_via_ai(
        pdf_path=PDF_PATH, vendor_id=vendor_config["vendor_id"], vendor_config=vendor_config,
        statement_id=STATEMENT_ID, statement_period=STATEMENT_PERIOD, source_file=SOURCE_FILE,
        extraction_service=extraction_service, validation_rules=validation_rules,
        extraction_config=extraction_config,
    )
    print(f"AI extraction: {len(statement_rows)} rows to Bronze, "
          f"{len(review_queue_rows)} flagged for review, {len(audit_rows)} AI call(s) logged.")
else:
    statement_rows = extract_vendor_statement(
        pdf_path=PDF_PATH, vendor_id=vendor_config["vendor_id"], vendor_config=vendor_config,
        statement_id=STATEMENT_ID, statement_period=STATEMENT_PERIOD, source_file=SOURCE_FILE,
    )
    review_queue_rows, audit_rows = [], []
    print(f"pdfplumber extraction (active_method={extraction_config['active_method']!r}): "
          f"{len(statement_rows)} rows to Bronze.")

print(f"Extracted {len(statement_rows)} vendor statement rows")

# ---- CELL 7: Validate extraction against the invoice_pattern config -----
# Config-driven sanity check: flag (don't fail) any extracted invoice
# number that doesn't look like this vendor's expected pattern -- an
# early warning for a bad extraction, not a hard stop. Applies identically
# regardless of which extraction path produced statement_rows.
invoice_pattern = re.compile(vendor_config["invoice_pattern"], re.IGNORECASE)
malformed = [r for r in statement_rows if not invoice_pattern.match(r.raw_invoice_number)]
print(f"Rows not matching expected invoice_pattern ({vendor_config['invoice_pattern']}): {len(malformed)}")
for r in malformed[:5]:
    print("  ", r.raw_invoice_number)

# ---- CELL 8: Internal ERP extraction -- adapter-gated ------------------
# As of this version, active_adapter = "mock_erp_generator" (see
# config/erp/internal_erp.json), so bronze_internal_erp_raw is populated
# by 03_mock_erp_generator.py instead, seeded from Silver VENDOR_STATEMENT
# rows -- not by this notebook. The Payment Voucher extraction function
# below is kept fully intact and dormant, not deleted: it was correct,
# tested code, and remains the right adapter for a possible future
# "voucher vs. statement" check, which is a different question from
# "ERP vs. statement." It simply doesn't run while a different adapter
# is active.
def extract_internal_erp_payment_voucher(pdf_path: str, vendor_id: str, erp_adapter: dict, statement_id: str, statement_period: str, source_file: str):
    """
    Line-based extraction for the "payment_voucher" adapter. This PDF has
    no row gridlines -- only column background rectangles -- so a naive
    table extraction misaligns as soon as a blank cell appears (see the
    credit line, which has no Orig Amount / Amount Due). pdfplumber's
    plain extract_text() already reconstructs each visual line correctly,
    so we parse that instead, using regex patterns from the ERP adapter
    config -- nothing here is asTech-specific beyond the patterns.

    DORMANT while active_adapter != "payment_voucher". See
    config/erp/internal_erp.json for why this adapter is retained
    rather than deleted.
    """
    invoice_re = re.compile(
        r"^(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+" +
        erp_adapter["invoice_extraction"]["invoice_pattern"].replace("(\\S+)", r"(?P<ref>\S+)") +
        r"\s+(?P<orig>[\d,.\-]+)\s+(?P<due>[\d,.\-]+)\s+(?P<applied>[\d,.\-]+)$"
    )
    credit_re = re.compile(
        r"^(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+" +
        erp_adapter["invoice_extraction"]["credit_pattern"].replace("(\\S+)", r"(?P<ref>\S+)") +
        r"\s+(?P<applied>[\d,.\-]+)$"
    )

    ingestion_ts = datetime.now(timezone.utc)
    rows_out = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            text = page.extract_text()
            row_number = 0
            for line in text.split("\n"):
                line = line.strip()
                m_invoice = invoice_re.match(line)
                m_credit = credit_re.match(line) if not m_invoice else None
                if not (m_invoice or m_credit):
                    continue
                row_number += 1
                m = m_invoice or m_credit
                rows_out.append(Row(
                    vendor_id=vendor_id,
                    source_file=source_file,
                    statement_id=statement_id,
                    statement_period=statement_period,
                    page_number=page_number,
                    row_number=row_number,
                    ingestion_timestamp=ingestion_ts,
                    raw_transaction_date=m.group("date"),
                    raw_description=line,
                    raw_amount_due=m.groupdict().get("due"),
                    raw_applied_amount=m.group("applied"),
                ))
    return rows_out


if active_adapter_name == "payment_voucher":
    erp_rows = extract_internal_erp_payment_voucher(
        pdf_path="sample_data/astech_payment_voucher_may2026.pdf",
        vendor_id=vendor_config["vendor_id"],
        erp_adapter=erp_adapter,
        statement_id="ASTECH-VOUCHER-2026-05",
        statement_period="2026-05",
        source_file="astech_payment_voucher_may2026.pdf",
    )
    print(f"Extracted {len(erp_rows)} Internal ERP (payment voucher) rows")
else:
    print(f"Skipping Payment Voucher extraction -- active adapter is '{active_adapter_name}', "
          f"not 'payment_voucher'. bronze_internal_erp_raw will be populated by 03_mock_erp_generator.py.")

# ---- CELL 9: Write vendor statement extract to Bronze --------------------
# Idempotent re-run pattern: delete any prior rows for this statement_id
# before inserting, so re-running this notebook for the same period
# doesn't duplicate data. (Requires Delta -- in Fabric this works as-is;
# for local Parquet testing this DELETE is skipped, see local_test copy.)
#
# An explicit schema is required here (not inference) because the AI
# metadata columns can be entirely NULL for a given run (always true for
# the pdfplumber path, possibly true for an all-fallback AI run) -- Spark
# cannot infer a type from a column of all-None values, and inference
# failing here would be a confusing error to hit for anyone reusing this
# pattern for a vendor whose statement also has no AI-supplied metadata.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType, DoubleType, BooleanType

bronze_statement_schema = StructType([
    StructField("vendor_id", StringType()),
    StructField("source_file", StringType()),
    StructField("statement_id", StringType()),
    StructField("statement_period", StringType()),
    StructField("page_number", IntegerType()),
    StructField("row_number", IntegerType()),
    StructField("ingestion_timestamp", TimestampType()),
    StructField("raw_invoice_date", StringType()),
    StructField("raw_invoice_number", StringType()),
    StructField("raw_work_order_number", StringType()),
    StructField("raw_ro_number", StringType()),
    StructField("raw_outstanding_amount", StringType()),
    StructField("raw_due_date", StringType()),
    StructField("raw_shop_name", StringType()),
    StructField("extraction_confidence", DecimalType(5, 4)),
    StructField("extraction_model", StringType()),
    StructField("document_section", StringType()),
    StructField("bounding_box", StringType()),
    StructField("raw_gemini_response", StringType()),
])

statement_df = spark.createDataFrame(statement_rows, schema=bronze_statement_schema)

try:
    spark.sql(f"DELETE FROM bronze_vendor_statement_raw WHERE statement_id = '{STATEMENT_ID}'")
except Exception as e:
    print(f"(DELETE skipped -- expected on non-Delta local test tables: {e})")

statement_df.write.mode("append").saveAsTable("bronze_vendor_statement_raw")
print("bronze_vendor_statement_raw written.")

if active_adapter_name == "payment_voucher":
    erp_df = spark.createDataFrame(erp_rows)
    try:
        spark.sql(f"DELETE FROM bronze_internal_erp_raw WHERE statement_id = 'ASTECH-VOUCHER-2026-05'")
    except Exception as e:
        print(f"(DELETE skipped -- expected on non-Delta local test tables: {e})")
    erp_df.write.mode("append").saveAsTable("bronze_internal_erp_raw")
    print("bronze_internal_erp_raw written (payment_voucher adapter).")

# ---- CELL 10: Write AI audit log (Phase B) -------------------------------
# Every AI call made this run -- success or failure -- gets one row here,
# regardless of what happened to the records it returned. Empty when the
# pdfplumber-only path ran (no AI calls were made).
ai_audit_log_schema = StructType([
    StructField("audit_id", StringType()),
    StructField("source_file", StringType()),
    StructField("vendor_id", StringType()),
    StructField("statement_id", StringType()),
    StructField("interaction_type", StringType()),
    StructField("ai_provider", StringType()),
    StructField("model", StringType()),
    StructField("prompt_version", StringType()),
    StructField("request_timestamp", TimestampType()),
    StructField("latency_ms", DoubleType()),
    StructField("attempt_count", IntegerType()),
    StructField("success", BooleanType()),
    StructField("response_status", StringType()),
    StructField("error_message", StringType()),
    StructField("extraction_confidence", DecimalType(5, 4)),
    StructField("validation_result", StringType()),
])

if audit_rows:
    for row in audit_rows:
        row["extraction_confidence"] = _to_decimal(row.get("extraction_confidence"))
    audit_df = spark.createDataFrame(audit_rows, schema=ai_audit_log_schema)
    try:
        spark.sql(f"DELETE FROM ai_audit_log WHERE statement_id = '{STATEMENT_ID}'")
    except Exception as e:
        print(f"(DELETE skipped -- expected on non-Delta local test tables: {e})")
    audit_df.write.mode("append").saveAsTable("ai_audit_log")
    print(f"ai_audit_log written: {len(audit_rows)} row(s).")
else:
    print("No AI calls made this run -- ai_audit_log untouched.")

# ---- CELL 11: Write validation document review queue (Phase B) ----------
# Anything that failed structural validation, fell below the confidence
# threshold, was flagged as a duplicate, or (on the AI path) came from a
# page whose AI call failed outright. Empty when the pdfplumber-only path
# ran, or when the AI path had a clean run.
review_queue_schema = StructType([
    StructField("review_id", StringType()),
    StructField("vendor_id", StringType()),
    StructField("source_file", StringType()),
    StructField("statement_id", StringType()),
    StructField("statement_period", StringType()),
    StructField("pipeline_stage", StringType()),
    StructField("rejection_category", StringType()),
    StructField("rejection_details", StringType()),
    StructField("extraction_confidence", DecimalType(5, 4)),
    StructField("confidence_threshold_applied", DecimalType(5, 4)),
    StructField("raw_payload", StringType()),
    StructField("ai_audit_id", StringType()),
    StructField("review_status", StringType()),
    StructField("flagged_timestamp", TimestampType()),
    StructField("reviewed_by", StringType()),
    StructField("reviewed_timestamp", TimestampType()),
    StructField("resolution_notes", StringType()),
])

if review_queue_rows:
    for row in review_queue_rows:
        row["extraction_confidence"] = _to_decimal(row.get("extraction_confidence"))
        row["confidence_threshold_applied"] = _to_decimal(row.get("confidence_threshold_applied"))
    review_df = spark.createDataFrame(review_queue_rows, schema=review_queue_schema)
    try:
        spark.sql(f"DELETE FROM validation_document_review_queue WHERE statement_id = '{STATEMENT_ID}'")
    except Exception as e:
        print(f"(DELETE skipped -- expected on non-Delta local test tables: {e})")
    review_df.write.mode("append").saveAsTable("validation_document_review_queue")
    print(f"validation_document_review_queue written: {len(review_queue_rows)} row(s) flagged for review.")
else:
    print("No records flagged for review this run.")

# ---- CELL 12: Validate what landed in Bronze ------------------------------
# NOTE: these hard asserts (202 rows, $13,860.79) reflect a PERFECT
# extraction of the sample PDF. They hold unconditionally on the
# pdfplumber_tabular path (unchanged from Phase A). On the ai_extraction
# path they hold only when Gemini (plus any per-page pdfplumber fallback)
# recovers every row -- a real run may legitimately land some rows in
# validation_document_review_queue instead, in which case these specific
# totals will not match and that is expected, not a bug; check
# validation_document_review_queue before assuming a regression.
from pyspark.sql import functions as F

print("\n--- bronze_vendor_statement_raw ---")
bvs = spark.table("bronze_vendor_statement_raw").filter(F.col("statement_id") == STATEMENT_ID)
bvs_count = bvs.count()
bvs_total = bvs.withColumn(
    "amt", F.regexp_replace(F.col("raw_outstanding_amount"), "[$, ]", "").cast("double")
).agg(F.sum("amt")).collect()[0][0]
print(f"Row count: {bvs_count}  |  Sum of raw_outstanding_amount: {round(bvs_total, 2)}")

print("\nExpected (from source PDF, verified independently): 202 invoice rows, $13,860.79.")
if extraction_config["active_method"] == "pdfplumber_tabular":
    assert bvs_count == 202, f"Expected 202 vendor statement rows, got {bvs_count}"
    assert round(bvs_total, 2) == 13860.79, f"Statement total mismatch: {bvs_total}"
    print("\nAll Phase 2 (vendor statement side) validation checks passed.")
else:
    if bvs_count == 202 and round(bvs_total, 2) == 13860.79:
        print("\nAI extraction recovered every row -- matches Phase 2's pdfplumber baseline exactly.")
    else:
        print(f"\nAI extraction landed {bvs_count} rows (vs. 202 expected) -- "
              f"check validation_document_review_queue for {STATEMENT_ID} before treating this as a regression.")
