# ==========================================================================
# Notebook: 02_silver_normalization_statement
# Purpose : Normalizes bronze_vendor_statement_raw into the shared
#           silver_reconciliation_standard schema, tagged
#           record_source = 'VENDOR_STATEMENT'.
#
# Why this runs BEFORE the Mock ERP Generator (03_mock_erp_generator.py):
#   The generator seeds itself from typed, cleaned statement data (real
#   dates, real decimal amounts) -- it cannot work off Bronze's raw
#   strings. This notebook has to complete first.
#
# Design notes:
#   - Every parsing rule comes from config/vendors/astech.json:
#     date_format, amount_format, revision_suffixes, invoice_pattern.
#     Nothing vendor-specific is hardcoded in this notebook.
#   - invoice_number_normalized uses the tested normalize_invoice_number()
#     function from Phase 1 (src/normalization.py), wrapped as a Spark UDF.
#   - statement_date (the document's own as-of date) comes from the
#     vendor config's period.end_date -- asTech's PDF has no separate
#     "statement date" distinct from individual invoice dates.
#   - posting_date and status stay NULL here -- they're ERP-side concepts,
#     populated only when the ERP side is normalized (Phase 4+).
#
# Phase B follow-up: Cell 7's validation no longer assumes every statement
# invoice always reaches Bronze. Under the AI extraction path, a record can
# legitimately be routed to validation_document_review_queue instead (low
# confidence, missing fields, duplicates) -- that is by design, not a
# defect, so a hard "must equal the full-statement total" assert is no
# longer correct. Validation now compares Silver against whatever Bronze
# actually contains, which still deterministically catches a real
# normalization bug (a row or amount silently dropped/changed going
# Bronze -> Silver) without assuming perfect upstream extraction.
#
# Mock ERP Generator follow-up: work_order_number is now carried through to
# Silver (it was captured in Bronze all along but dropped here). The
# generator's vendor_reference_issue scenario needs the REAL work order
# number -- asTech's actual SIN12307276 / 24419074 data-quality case -- not
# a synthesized stand-in, to mean anything.
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
import sys
sys.path.insert(0, ".")
from src.normalization import make_spark_udf
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# ---- CELL 2: Load vendor config -- every parsing rule traces back here --
with open("config/vendors/astech.json") as f:
    vendor_config = json.load(f)

date_fmt = vendor_config["date_format"]                    # e.g. "MM/dd/yyyy"
revision_suffixes = vendor_config["revision_suffixes"]      # e.g. ["X\\d+$", "-\\d+$", "R$"]
statement_date_value = vendor_config["period"]["end_date"]  # e.g. "2026-05-31"

print(f"Normalizing {vendor_config['vendor_name']} using date_format={date_fmt}")
print(f"Revision suffixes: {revision_suffixes}")

# ---- CELL 3: Read Bronze, scoped to this vendor + period -----------------
bronze = spark.table("bronze_vendor_statement_raw").filter(
    (F.col("vendor_id") == vendor_config["vendor_id"]) &
    (F.col("statement_period") == vendor_config["period"]["end_date"][:7])
)
bronze_count = bronze.count()
print(f"Read {bronze_count} raw rows from bronze_vendor_statement_raw")

# ---- CELL 4: Apply invoice number normalization (config-driven UDF) -----
normalize_udf = make_spark_udf()
suffix_array = F.array([F.lit(p) for p in revision_suffixes])

# ---- CELL 5: Transform Bronze -> Silver standard schema ------------------
silver_new = (
    bronze
    .withColumn("record_id", F.sha2(F.concat_ws("|",
        F.lit("VENDOR_STATEMENT"), F.col("vendor_id"), F.col("raw_invoice_number"),
        F.col("raw_outstanding_amount"), F.col("statement_id")), 256))
    .withColumn("record_source", F.lit("VENDOR_STATEMENT"))
    .withColumn("document_type", F.lit("VENDOR_STATEMENT"))
    .withColumn("statement_date", F.to_date(F.lit(statement_date_value)))
    .withColumn("vendor_name", F.lit(vendor_config["vendor_name"]))
    .withColumn("shop", F.col("raw_shop_name"))
    .withColumn("invoice_number", F.col("raw_invoice_number"))
    .withColumn("invoice_number_normalized", normalize_udf(F.col("raw_invoice_number"), suffix_array))
    .withColumn("invoice_date", F.to_date(F.col("raw_invoice_date"), date_fmt))
    .withColumn("ro_number", F.col("raw_ro_number"))
    .withColumn("work_order_number", F.col("raw_work_order_number"))
    .withColumn("po_number", F.lit(None).cast("string"))   # asTech's statement has no PO column
    .withColumn("outstanding_amount",
        F.regexp_replace(F.col("raw_outstanding_amount"), r"[^\d\.\-]", "").cast(DecimalType(12, 2)))
    .withColumn("amount", F.col("outstanding_amount"))     # statement doesn't distinguish original vs. outstanding
    .withColumn("credit", F.lit(None).cast(DecimalType(12, 2)))  # no credit lines on asTech's clean tabular statement
    .withColumn("due_date", F.to_date(F.col("raw_due_date"), date_fmt))
    .withColumn("posting_date", F.lit(None).cast("date"))  # ERP-side concept -- not applicable here
    .withColumn("status", F.lit(None).cast("string"))      # ERP-side concept -- not applicable here
    .withColumn("description", F.lit(None).cast("string"))
    .select(
        "record_id", "record_source", "document_type", "statement_id", "statement_date",
        "vendor_id", "vendor_name", "shop", "invoice_number", "invoice_number_normalized",
        "invoice_date", "ro_number", "work_order_number", "po_number", "amount", "credit",
        "outstanding_amount", "due_date", "posting_date", "status", "description",
        "statement_period", "source_file", "ingestion_timestamp",
    )
)

# ---- CELL 6: Idempotent write -- delete this statement_id, then insert --
statement_id = vendor_config.get("statement_id", None)
target_statement_ids = [row["statement_id"] for row in bronze.select("statement_id").distinct().collect()]

try:
    ids_literal = ", ".join(f"'{sid}'" for sid in target_statement_ids)
    spark.sql(f"""
        DELETE FROM silver_reconciliation_standard
        WHERE record_source = 'VENDOR_STATEMENT' AND statement_id IN ({ids_literal})
    """)
except Exception as e:
    print(f"(DELETE skipped -- expected on non-Delta local test tables: {e})")

silver_new.write.mode("append").saveAsTable("silver_reconciliation_standard")
print("Silver normalization (VENDOR_STATEMENT side) written.")

# ---- CELL 7: Validate -----------------------------------------------------
# bronze_total is computed the same way 01_bronze_ingestion.py's own Bronze
# validation computes it (strip $/,/spaces, cast to double) -- Silver's
# total must match BRONZE'S total, not a hardcoded full-statement figure,
# since Bronze itself may legitimately hold fewer than the full statement's
# rows when the AI path has routed some to validation_document_review_queue.
bronze_total = bronze.withColumn(
    "amt", F.regexp_replace(F.col("raw_outstanding_amount"), "[$, ]", "").cast("double")
).agg(F.sum("amt")).collect()[0][0]

silver_check = spark.table("silver_reconciliation_standard").filter(
    (F.col("record_source") == "VENDOR_STATEMENT") &
    (F.col("statement_period") == vendor_config["period"]["end_date"][:7])
)
silver_count = silver_check.count()
silver_total = silver_check.agg(F.sum("outstanding_amount")).collect()[0][0]
null_dates = silver_check.filter(F.col("invoice_date").isNull()).count()
null_amounts = silver_check.filter(F.col("outstanding_amount").isNull()).count()

review_queue_count = 0
if target_statement_ids:  # computed once already, in Cell 6
    review_queue_count = spark.table("validation_document_review_queue").filter(
        F.col("statement_id").isin(target_statement_ids)
    ).count()

print(f"Bronze row count (this run's extraction): {bronze_count}  |  Bronze total: {bronze_total}")
print(f"Silver row count: {silver_count}")
print(f"Silver total outstanding_amount: {silver_total}")
print(f"Rows with unparsed invoice_date: {null_dates}")
print(f"Rows with unparsed outstanding_amount: {null_amounts}")
print(f"Rows flagged in validation_document_review_queue for {target_statement_ids} "
      f"(informational only -- these were intentionally excluded from Bronze, not a normalization failure): {review_queue_count}")

sample_normalized = silver_check.select("invoice_number", "invoice_number_normalized").limit(3).collect()
print("\nSample invoice_number -> invoice_number_normalized (unchanged for asTech, no revisions in this statement):")
for r in sample_normalized:
    print(f"  {r['invoice_number']} -> {r['invoice_number_normalized']}")

# Genuine data-integrity checks -- these must ALWAYS hold, regardless of
# which extraction path populated Bronze or how many rows (if any) the AI
# path routed to the review queue instead of Bronze:
assert bronze_count > 0, "Bronze has zero rows for this vendor/period -- extraction produced nothing to normalize."
assert silver_count == bronze_count, f"Row count changed during normalization: {bronze_count} -> {silver_count}"
assert bronze_total is not None and round(float(silver_total), 2) == round(float(bronze_total), 2), \
    f"Total drifted during normalization: bronze={bronze_total}, silver={silver_total}"
assert null_dates == 0, f"{null_dates} rows failed date parsing"
assert null_amounts == 0, f"{null_amounts} rows failed amount parsing"

FULL_STATEMENT_ROW_COUNT, FULL_STATEMENT_TOTAL = 202, 13860.79
if bronze_count == FULL_STATEMENT_ROW_COUNT and round(bronze_total, 2) == FULL_STATEMENT_TOTAL:
    print(f"\nBronze matches the full-statement baseline ({FULL_STATEMENT_ROW_COUNT} rows, "
          f"${FULL_STATEMENT_TOTAL}) -- extraction recovered every invoice this run.")
else:
    print(f"\nBronze holds {bronze_count} rows (vs. the {FULL_STATEMENT_ROW_COUNT}-row full-statement "
          f"baseline) -- expected whenever the AI path routes records to validation_document_review_queue; "
          f"see that table for {target_statement_ids} before treating this as a regression.")

print("\nAll Phase 3 (vendor statement side) validation checks passed.")
