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
#           fully intact, gated behind an adapter check -- see Cell 5.
#
# Design notes:
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
import pdfplumber
from datetime import datetime, timezone
from pyspark.sql import Row

# ---- CELL 2: Load config -- nothing vendor-specific is hardcoded below --
# In Fabric, config files live in the Lakehouse's Files/ area; locally
# they're read straight from the project's config/ folder. Either way,
# every extraction decision below traces back to one of these two files.
VENDOR_CONFIG_PATH = "config/vendors/astech.json"
ERP_CONFIG_PATH = "config/erp/internal_erp.json"

with open(VENDOR_CONFIG_PATH) as f:
    vendor_config = json.load(f)

with open(ERP_CONFIG_PATH) as f:
    erp_config = json.load(f)

active_adapter_name = erp_config["active_adapter"]                 # "payment_voucher" for this PoC
erp_adapter = erp_config["adapters"][active_adapter_name]

print(f"Vendor: {vendor_config['vendor_name']} ({vendor_config['vendor_id']})")
print(f"Active ERP adapter: {active_adapter_name} -> document_type = {erp_adapter['document_type']}")

# ---- CELL 3: Extract the Vendor Statement (tabular, has real gridlines) --
def extract_vendor_statement(pdf_path: str, vendor_id: str, vendor_config: dict, statement_id: str, statement_period: str, source_file: str):
    """
    Table-based extraction for vendors whose statement_format is
    "tabular_pdf" AND whose PDF actually has detectable row gridlines
    (asTech qualifies -- see Phase 2 validation notes for vendors that
    don't, like Quirk, which will need a different extractor function
    registered against a different statement_format value later).
    """
    shop_pattern = re.compile(vendor_config["shop_extraction"]["pattern"])
    ingestion_ts = datetime.now(timezone.utc)
    rows_out = []

    with pdfplumber.open(pdf_path) as pdf:
        # Shop name lives in the header text of page 1, not in the table.
        header_text = pdf.pages[0].extract_text()
        shop_match = shop_pattern.search(header_text)
        shop_name = shop_match.group(1).strip() if shop_match else None

        for page_number, page in enumerate(pdf.pages, start=1):
            table = page.extract_table()
            if not table:
                continue
            for row_number, row in enumerate(table, start=1):
                # Skip the header row (only present on page 1) and the
                # "Total Outstanding Invoices:" footer row (last page).
                if row[0] in (None, "", "Invoice Date"):
                    continue
                if row[0] and str(row[0]).startswith("Total"):
                    continue
                if not row[1]:  # no invoice number -> not a real data row
                    continue

                rows_out.append(Row(
                    vendor_id=vendor_id,
                    source_file=source_file,
                    statement_id=statement_id,
                    statement_period=statement_period,
                    page_number=int(page_number),
                    row_number=int(row_number),
                    ingestion_timestamp=ingestion_ts,
                    raw_invoice_date=row[0],
                    raw_invoice_number=row[1],
                    raw_work_order_number=row[2],
                    raw_ro_number=row[3],
                    raw_outstanding_amount=row[4],
                    raw_due_date=row[5],
                    raw_shop_name=shop_name,
                    # AI metadata columns -- NULL for this extraction path.
                    # pdfplumber has no concept of extraction confidence,
                    # model, semantic section, or bounding boxes. Nothing
                    # downstream breaks: these columns are nullable by
                    # design specifically to support providers/paths that
                    # don't supply them.
                    extraction_confidence=None,
                    extraction_model=None,
                    document_section=None,
                    bounding_box=None,
                    raw_gemini_response=None,
                ))
    return rows_out


statement_rows = extract_vendor_statement(
    pdf_path="sample_data/astech_vendor_statement_may2026.pdf",
    vendor_id=vendor_config["vendor_id"],
    vendor_config=vendor_config,
    statement_id="ASTECH-COLLEX-2026-05",
    statement_period="2026-05",
    source_file="astech_vendor_statement_may2026.pdf",
)
print(f"Extracted {len(statement_rows)} vendor statement rows")

# ---- CELL 4: Validate extraction against the invoice_pattern config -----
# Config-driven sanity check: flag (don't fail) any extracted invoice
# number that doesn't look like this vendor's expected pattern -- an
# early warning for a bad extraction, not a hard stop.
invoice_pattern = re.compile(vendor_config["invoice_pattern"], re.IGNORECASE)
malformed = [r for r in statement_rows if not invoice_pattern.match(r.raw_invoice_number)]
print(f"Rows not matching expected invoice_pattern ({vendor_config['invoice_pattern']}): {len(malformed)}")
for r in malformed[:5]:
    print("  ", r.raw_invoice_number)

# ---- CELL 5: Internal ERP extraction -- adapter-gated ------------------
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

# ---- CELL 6: Write vendor statement extract to Bronze --------------------
# Idempotent re-run pattern: delete any prior rows for this statement_id
# before inserting, so re-running this notebook for the same period
# doesn't duplicate data. (Requires Delta -- in Fabric this works as-is;
# for local Parquet testing this DELETE is skipped, see local_test copy.)
#
# An explicit schema is required here (not inference) because the AI
# metadata columns are entirely NULL for this extraction path -- Spark
# cannot infer a type from a column of all-None values, and inference
# failing here would be a confusing error to hit for anyone reusing this
# pattern for a vendor whose statement also has no AI-supplied metadata.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType

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
    spark.sql(f"DELETE FROM bronze_vendor_statement_raw WHERE statement_id = 'ASTECH-COLLEX-2026-05'")
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

# ---- CELL 7: Validate what landed in Bronze ------------------------------
from pyspark.sql import functions as F

print("\n--- bronze_vendor_statement_raw ---")
bvs = spark.table("bronze_vendor_statement_raw").filter(F.col("statement_id") == "ASTECH-COLLEX-2026-05")
bvs_count = bvs.count()
bvs_total = bvs.withColumn(
    "amt", F.regexp_replace(F.col("raw_outstanding_amount"), "[$, ]", "").cast("double")
).agg(F.sum("amt")).collect()[0][0]
print(f"Row count: {bvs_count}  |  Sum of raw_outstanding_amount: {round(bvs_total, 2)}")

print("\nExpected (from source PDF, verified independently): 202 invoice rows, $13,860.79.")
assert bvs_count == 202, f"Expected 202 vendor statement rows, got {bvs_count}"
assert round(bvs_total, 2) == 13860.79, f"Statement total mismatch: {bvs_total}"
print("\nAll Phase 2 (vendor statement side) validation checks passed.")
