# ==========================================================================
# Notebook: 04_silver_normalization_erp
# Purpose : Normalizes bronze_internal_erp_raw into the shared
#           silver_reconciliation_standard schema, tagged
#           record_source = 'INTERNAL_ERP'. Mirrors
#           02_silver_normalization_statement.py's structure and validation
#           philosophy exactly, for the other side of the reconciliation.
#
# Design notes:
#   - invoice_number_normalized reuses src/normalization.py's tested
#     normalize_invoice_number() (via make_spark_udf()) with the SAME
#     vendor-configured revision_suffixes as the statement side -- this
#     matters concretely here: the Mock ERP Generator's invoice_revision
#     scenario reissues the ERP-side invoice number with a suffix, and the
#     Matching Engine (Phase 4+) will join on invoice_number_normalized,
#     not the raw invoice_number, to resolve exactly that case.
#   - posting_date and status are POPULATED here (Bronze actually carries
#     them for the ERP side) -- the mirror image of the statement side,
#     where they stay NULL. due_date and work_order_number are the
#     opposite: NULL here, since they're statement-side-only concepts (the
#     ERP extract shape has no due-date or work-order column).
#   - credit is derived generically from amount vs. outstanding_amount --
#     whichever is larger implies a credit was applied -- with no
#     knowledge of "missing_credit" as a mock-generator-specific concept.
#     This is deliberate: it keeps working unchanged once a real NetSuite
#     feed replaces the generator, since a real feed would produce the
#     same amount/outstanding_amount shape for a genuine credit memo.
#   - record_id's hash INCLUDES row_number, unlike the statement side's
#     hash. Without it, the duplicate_invoice scenario's two intentionally
#     identical ERP rows (same invoice_number, same amount, same
#     statement_id) would collide on the same surrogate key. row_number is
#     exactly the "generation sequence" column Bronze already carries for
#     generated data, for this purpose.
#   - Like the statement side (see that notebook's Phase B follow-up),
#     validation compares Silver against whatever Bronze actually
#     contains -- there is no fixed "full extract" baseline here at all,
#     since the ERP extract's size is a function of the scenario mix, not
#     a source document with an independently known invoice count.
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

# ---- CELL 2: Load vendor + ERP adapter config ----------------------------
with open("config/vendors/astech.json") as f:
    vendor_config = json.load(f)

with open("config/erp/internal_erp.json") as f:
    erp_config = json.load(f)

erp_adapter = erp_config["adapters"][erp_config["active_adapter"]]

date_fmt = vendor_config["date_format"]                    # e.g. "MM/dd/yyyy" -- same convention both sides render/parse
revision_suffixes = vendor_config["revision_suffixes"]
statement_date_value = vendor_config["period"]["end_date"]
statement_period = statement_date_value[:7]

print(f"Normalizing {vendor_config['vendor_name']} INTERNAL_ERP side using date_format={date_fmt}")
print(f"ERP adapter: {erp_config['active_adapter']} -> document_type = {erp_adapter['document_type']}")

# ---- CELL 3: Read Bronze, scoped to this vendor + period -----------------
bronze = spark.table("bronze_internal_erp_raw").filter(
    (F.col("vendor_id") == vendor_config["vendor_id"]) &
    (F.col("statement_period") == statement_period)
)
bronze_count = bronze.count()
print(f"Read {bronze_count} raw rows from bronze_internal_erp_raw")

# ---- CELL 4: Apply invoice number normalization (config-driven UDF) -----
# Reused, not reimplemented -- same function, same vendor-configured
# suffixes as 02_silver_normalization_statement.py.
normalize_udf = make_spark_udf()
suffix_array = F.array([F.lit(p) for p in revision_suffixes])

# ---- CELL 5: Transform Bronze -> Silver standard schema ------------------
silver_new = (
    bronze
    .withColumn("record_source", F.lit("INTERNAL_ERP"))
    .withColumn("document_type", F.lit(erp_adapter["document_type"]))
    .withColumn("statement_date", F.to_date(F.lit(statement_date_value)))
    .withColumn("vendor_name", F.col("raw_vendor"))
    .withColumn("shop", F.col("raw_shop"))
    .withColumn("invoice_number", F.col("raw_invoice_number"))
    .withColumn("invoice_number_normalized", normalize_udf(F.col("raw_invoice_number"), suffix_array))
    .withColumn("invoice_date", F.to_date(F.col("raw_invoice_date"), date_fmt))
    .withColumn("ro_number", F.col("raw_ro_number"))
    .withColumn("work_order_number", F.lit(None).cast("string"))  # statement-side-only concept
    .withColumn("po_number", F.col("raw_po_number"))
    .withColumn("amount",
        F.regexp_replace(F.col("raw_amount"), r"[^\d\.\-]", "").cast(DecimalType(12, 2)))
    .withColumn("outstanding_amount",
        F.regexp_replace(F.col("raw_outstanding_amount"), r"[^\d\.\-]", "").cast(DecimalType(12, 2)))
    .withColumn("credit",
        F.when(F.col("amount") > F.col("outstanding_amount"), F.col("amount") - F.col("outstanding_amount"))
         .otherwise(F.lit(None).cast(DecimalType(12, 2))))
    .withColumn("due_date", F.lit(None).cast("date"))       # statement-side-only concept
    .withColumn("posting_date", F.to_date(F.col("raw_posting_date"), date_fmt))  # NULL for PENDING rows -- expected
    .withColumn("status", F.col("raw_status"))
    .withColumn("description", F.lit(None).cast("string"))
    .withColumn("record_id", F.sha2(F.concat_ws("|",
        F.lit("INTERNAL_ERP"), F.col("vendor_id"), F.col("raw_invoice_number"),
        F.col("raw_outstanding_amount"), F.col("statement_id"), F.col("row_number")), 256))
    .select(
        "record_id", "record_source", "document_type", "statement_id", "statement_date",
        "vendor_id", "vendor_name", "shop", "invoice_number", "invoice_number_normalized",
        "invoice_date", "ro_number", "work_order_number", "po_number", "amount", "credit",
        "outstanding_amount", "due_date", "posting_date", "status", "description",
        "statement_period", "source_file", "ingestion_timestamp",
    )
)

# ---- CELL 6: Idempotent write -- delete this statement_id, then insert --
target_statement_ids = [row["statement_id"] for row in bronze.select("statement_id").distinct().collect()]

try:
    ids_literal = ", ".join(f"'{sid}'" for sid in target_statement_ids)
    spark.sql(f"""
        DELETE FROM silver_reconciliation_standard
        WHERE record_source = 'INTERNAL_ERP' AND statement_id IN ({ids_literal})
    """)
except Exception as e:
    print(f"(DELETE skipped -- expected on non-Delta local test tables: {e})")

silver_new.write.mode("append").saveAsTable("silver_reconciliation_standard")
print("Silver normalization (INTERNAL_ERP side) written.")

# ---- CELL 7: Validate -----------------------------------------------------
# No hardcoded totals at all here -- unlike the statement side, there is no
# independently-known "full extract" baseline; the ERP extract's size is a
# function of the scenario mix applied to however many statement invoices
# existed. Validation instead confirms Silver faithfully reflects whatever
# Bronze actually contains, which still deterministically catches a real
# normalization bug.
bronze_total = bronze.withColumn(
    "amt", F.regexp_replace(F.col("raw_outstanding_amount"), "[$, ]", "").cast("double")
).agg(F.sum("amt")).collect()[0][0]

silver_check = spark.table("silver_reconciliation_standard").filter(
    (F.col("record_source") == "INTERNAL_ERP") &
    (F.col("statement_period") == statement_period)
)
silver_count = silver_check.count()
silver_total = silver_check.agg(F.sum("outstanding_amount")).collect()[0][0]
null_invoice_dates = silver_check.filter(F.col("invoice_date").isNull()).count()
null_outstanding = silver_check.filter(F.col("outstanding_amount").isNull()).count()

# posting_date must be NULL if and only if status = 'PENDING' -- NOT a
# blanket null-count-must-be-zero check, since NULL posting_date is the
# correct, expected state for a pending invoice.
posted_missing_posting_date = silver_check.filter(
    (F.col("posting_date").isNull()) & (F.col("status") != F.lit("PENDING"))
).count()
pending_with_posting_date = silver_check.filter(
    (F.col("status") == F.lit("PENDING")) & (F.col("posting_date").isNotNull())
).count()

print(f"Bronze row count: {bronze_count}  |  Bronze total: {bronze_total}")
print(f"Silver row count: {silver_count}  |  Silver total outstanding_amount: {silver_total}")
print(f"Rows with unparsed invoice_date: {null_invoice_dates}")
print(f"Rows with unparsed outstanding_amount: {null_outstanding}")
print(f"POSTED/other rows missing a posting_date (should be 0): {posted_missing_posting_date}")
print(f"PENDING rows that unexpectedly have a posting_date (should be 0): {pending_with_posting_date}")

sample_normalized = silver_check.filter(F.col("invoice_number") != F.col("invoice_number_normalized")) \
    .select("invoice_number", "invoice_number_normalized").limit(3).collect()
print("\nSample revised invoice_number -> invoice_number_normalized (invoice_revision scenario rows, if any):")
for r in sample_normalized:
    print(f"  {r['invoice_number']} -> {r['invoice_number_normalized']}")

assert bronze_count > 0, "Bronze has zero rows for this vendor/period -- run 03_mock_erp_generator.py first."
assert silver_count == bronze_count, f"Row count changed during normalization: {bronze_count} -> {silver_count}"
assert bronze_total is not None and round(float(silver_total), 2) == round(float(bronze_total), 2), \
    f"Total drifted during normalization: bronze={bronze_total}, silver={silver_total}"
assert null_invoice_dates == 0, f"{null_invoice_dates} rows failed invoice_date parsing"
assert null_outstanding == 0, f"{null_outstanding} rows failed outstanding_amount parsing"
assert posted_missing_posting_date == 0, f"{posted_missing_posting_date} non-PENDING rows are missing a posting_date"
assert pending_with_posting_date == 0, f"{pending_with_posting_date} PENDING rows unexpectedly have a posting_date"

print("\nAll Silver normalization (INTERNAL_ERP side) validation checks passed.")
