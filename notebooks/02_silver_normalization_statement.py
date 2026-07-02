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
        "invoice_date", "ro_number", "po_number", "amount", "credit", "outstanding_amount",
        "due_date", "posting_date", "status", "description", "statement_period",
        "source_file", "ingestion_timestamp",
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
silver_check = spark.table("silver_reconciliation_standard").filter(
    (F.col("record_source") == "VENDOR_STATEMENT") &
    (F.col("statement_period") == vendor_config["period"]["end_date"][:7])
)
silver_count = silver_check.count()
silver_total = silver_check.agg(F.sum("outstanding_amount")).collect()[0][0]
null_dates = silver_check.filter(F.col("invoice_date").isNull()).count()
null_amounts = silver_check.filter(F.col("outstanding_amount").isNull()).count()

print(f"Silver row count: {silver_count}")
print(f"Silver total outstanding_amount: {silver_total}")
print(f"Rows with unparsed invoice_date: {null_dates}")
print(f"Rows with unparsed outstanding_amount: {null_amounts}")

sample_normalized = silver_check.select("invoice_number", "invoice_number_normalized").limit(3).collect()
print("\nSample invoice_number -> invoice_number_normalized (unchanged for asTech, no revisions in this statement):")
for r in sample_normalized:
    print(f"  {r['invoice_number']} -> {r['invoice_number_normalized']}")

assert silver_count == bronze_count, f"Row count changed during normalization: {bronze_count} -> {silver_count}"
assert round(float(silver_total), 2) == 13860.79, f"Total mismatch after normalization: {silver_total}"
assert null_dates == 0, f"{null_dates} rows failed date parsing"
assert null_amounts == 0, f"{null_amounts} rows failed amount parsing"
print("\nAll Phase 3 validation checks passed.")
