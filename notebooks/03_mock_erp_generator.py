# ==========================================================================
# Notebook: 03_mock_erp_generator
# Purpose : Simulates a realistic NetSuite export (the "Internal ERP
#           Dataset") since a real ERP feed isn't available for this PoC.
#           Reads Silver VENDOR_STATEMENT rows, assigns each invoice a
#           reconciliation scenario, mutates it accordingly, and writes the
#           result to bronze_internal_erp_raw plus a ground-truth row per
#           invoice to validation_mutation_manifest.
#
# No AI anywhere in this notebook. Deterministic, config-driven, seeded --
# see config/erp/internal_erp.json (adapter wiring: seed_source,
# scenario_config_path, output_target) and
# config/mock_erp/astech_scenarios.json (scenario mix, mutation parameters,
# expected outcomes, random_seed -- the single source of truth for
# everything this notebook does; nothing scenario-specific is hardcoded
# here or in src/mock_erp/).
#
# Design notes:
#   - All real logic lives in src/mock_erp/ (scenario_assignment.py,
#     mutations.py, generator.py) -- pure Python, unit-tested without
#     Spark. This notebook is a thin orchestrator: read Silver, call
#     generate_mock_erp(), map its plain-dict output to Spark Rows, write.
#   - Determinism depends on TWO things together: random.Random(seed) in
#     src/mock_erp/, AND a fully tie-broken .orderBy(...) here before
#     .collect() -- Spark's collect() order is not guaranteed stable
#     across runs on its own. record_id (a hash, always present) is the
#     final tiebreaker.
#   - raw_invoice_date / raw_posting_date are rendered as MM/dd/yyyy
#     strings (asTech's specific date_format) so 04_silver_normalization_erp.py
#     can parse them back with the exact same to_date(..., date_fmt) call
#     02_silver_normalization_statement.py already uses for the statement
#     side -- one shared parsing convention regardless of which side
#     produced the string.
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
import uuid
from datetime import datetime, timezone
from pyspark.sql import Row, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

sys.path.insert(0, ".")
from src.mock_erp.generator import generate_mock_erp

# ---- CELL 2: Load config -- scenario mix, mutation params, adapter wiring
VENDOR_CONFIG_PATH = "config/vendors/astech.json"
ERP_CONFIG_PATH = "config/erp/internal_erp.json"

with open(VENDOR_CONFIG_PATH) as f:
    vendor_config = json.load(f)

with open(ERP_CONFIG_PATH) as f:
    erp_config = json.load(f)

erp_adapter = erp_config["adapters"]["mock_erp_generator"]
with open(erp_adapter["scenario_config_path"]) as f:
    scenarios_config = json.load(f)

VENDOR_ID = vendor_config["vendor_id"]
STATEMENT_PERIOD = vendor_config["period"]["end_date"][:7]
ERP_STATEMENT_ID = f"ASTECH-MOCKERP-{STATEMENT_PERIOD}"
SOURCE_FILE = f"mock_erp_generator_v{scenarios_config['generator_version']}"

print(f"Vendor: {vendor_config['vendor_name']} ({VENDOR_ID})")
print(f"Scenario config: {erp_adapter['scenario_config_path']} (generator_version={scenarios_config['generator_version']}, seed={scenarios_config['random_seed']})")

# ---- CELL 3: Read Silver VENDOR_STATEMENT rows, deterministically ordered
# .orderBy(...) with a fully tie-broken key is what makes the seeded
# generation reproducible run to run -- collect() alone does not guarantee
# row order. record_id (a hash of source+vendor+invoice+amount+statement_id)
# is always present and effectively unique, so it's the final tiebreaker.
statement_rows = (
    spark.table("silver_reconciliation_standard")
    .filter(
        (F.col("record_source") == "VENDOR_STATEMENT") &
        (F.col("vendor_id") == VENDOR_ID) &
        (F.col("statement_period") == STATEMENT_PERIOD)
    )
    .select(
        "vendor_id", "vendor_name", "shop", "invoice_number", "invoice_date",
        "ro_number", "work_order_number", "outstanding_amount",
        "statement_id", "statement_period", "record_id",
    )
    .orderBy("invoice_number", "ro_number", "record_id")
    .collect()
)
statement_invoices = [row.asDict() for row in statement_rows]
print(f"Read {len(statement_invoices)} Silver VENDOR_STATEMENT rows to seed the generator")

# ---- CELL 4: Generate the mock ERP extract (pure Python, no Spark) -------
result = generate_mock_erp(statement_invoices, scenarios_config, seed=scenarios_config["random_seed"])
print(f"Generated {len(result.bronze_records)} ERP-side Bronze rows "
      f"and {len(result.manifest_records)} manifest rows from {len(statement_invoices)} statement invoices")

# ---- CELL 5: Map generator output to Bronze / manifest Spark Rows -------
# Explicit schemas throughout: page_number is always NULL for generated
# data (no PDF page), and posting_date is NULL for every PENDING row --
# Spark cannot infer a type from an all-NULL column, same reasoning as
# 01_bronze_ingestion.py's bronze_statement_schema.
ingestion_ts = datetime.now(timezone.utc)
DATE_RENDER_FORMAT = "%m/%d/%Y"  # matches asTech's date_format ("MM/dd/yyyy") for round-trip parsing in Silver

erp_bronze_schema = StructType([
    StructField("vendor_id", StringType()),
    StructField("source_file", StringType()),
    StructField("statement_id", StringType()),
    StructField("statement_period", StringType()),
    StructField("page_number", IntegerType()),
    StructField("row_number", IntegerType()),
    StructField("ingestion_timestamp", TimestampType()),
    StructField("raw_vendor", StringType()),
    StructField("raw_invoice_number", StringType()),
    StructField("raw_invoice_date", StringType()),
    StructField("raw_posting_date", StringType()),
    StructField("raw_amount", StringType()),
    StructField("raw_outstanding_amount", StringType()),
    StructField("raw_ro_number", StringType()),
    StructField("raw_po_number", StringType()),
    StructField("raw_shop", StringType()),
    StructField("raw_status", StringType()),
])

erp_bronze_rows = [
    Row(
        vendor_id=record["vendor_id"],
        source_file=SOURCE_FILE,
        statement_id=ERP_STATEMENT_ID,
        statement_period=record["statement_period"],
        page_number=None,
        row_number=record["row_number"],
        ingestion_timestamp=ingestion_ts,
        raw_vendor=record["vendor"],
        raw_invoice_number=record["invoice_number"],
        raw_invoice_date=record["invoice_date"].strftime(DATE_RENDER_FORMAT),
        raw_posting_date=record["posting_date"].strftime(DATE_RENDER_FORMAT) if record["posting_date"] else None,
        raw_amount=record["amount"],
        raw_outstanding_amount=record["outstanding_amount"],
        raw_ro_number=record["ro_number"],
        raw_po_number=record["po_number"],
        raw_shop=record["shop"],
        raw_status=record["status"],
    )
    for record in result.bronze_records
]

manifest_schema = StructType([
    StructField("manifest_id", StringType()),
    StructField("vendor_id", StringType()),
    StructField("statement_period", StringType()),
    StructField("statement_invoice_number", StringType()),
    StructField("generated_erp_invoice_number", StringType()),
    StructField("scenario_type", StringType()),
    StructField("expected_match_status", StringType()),
    StructField("expected_match_level", IntegerType()),
    StructField("expected_exception_reason", StringType()),
    StructField("mutation_details", StringType()),
    StructField("generator_config_version", StringType()),
    StructField("generation_timestamp", TimestampType()),
])

manifest_rows = [
    Row(
        manifest_id=str(uuid.uuid4()),
        vendor_id=record["vendor_id"],
        statement_period=record["statement_period"],
        statement_invoice_number=record["statement_invoice_number"],
        generated_erp_invoice_number=record["generated_erp_invoice_number"],
        scenario_type=record["scenario_type"],
        expected_match_status=record["expected_match_status"],
        expected_match_level=record["expected_match_level"],
        expected_exception_reason=record["expected_exception_reason"],
        mutation_details=record["mutation_details"],
        generator_config_version=record["generator_config_version"],
        generation_timestamp=ingestion_ts,
    )
    for record in result.manifest_records
]

# ---- CELL 6: Idempotent writes -------------------------------------------
erp_bronze_df = spark.createDataFrame(erp_bronze_rows, schema=erp_bronze_schema)
try:
    spark.sql(f"DELETE FROM bronze_internal_erp_raw WHERE statement_id = '{ERP_STATEMENT_ID}'")
except Exception as e:
    print(f"(DELETE skipped -- expected on non-Delta local test tables: {e})")
erp_bronze_df.write.mode("append").saveAsTable("bronze_internal_erp_raw")
print(f"bronze_internal_erp_raw written: {len(erp_bronze_rows)} row(s).")

manifest_df = spark.createDataFrame(manifest_rows, schema=manifest_schema)
try:
    spark.sql(f"""
        DELETE FROM validation_mutation_manifest
        WHERE vendor_id = '{VENDOR_ID}' AND statement_period = '{STATEMENT_PERIOD}'
    """)
except Exception as e:
    print(f"(DELETE skipped -- expected on non-Delta local test tables: {e})")
manifest_df.write.mode("append").saveAsTable("validation_mutation_manifest")
print(f"validation_mutation_manifest written: {len(manifest_rows)} row(s).")

# ---- CELL 7: Validate -----------------------------------------------------
# No hardcoded totals -- this generator's "correct" output size depends on
# how many statement invoices existed to seed it, which the AI extraction
# path (Phase B) can itself legitimately vary run to run.
written_bronze_count = spark.table("bronze_internal_erp_raw").filter(F.col("statement_id") == ERP_STATEMENT_ID).count()
written_manifest_count = spark.table("validation_mutation_manifest").filter(
    (F.col("vendor_id") == VENDOR_ID) & (F.col("statement_period") == STATEMENT_PERIOD)
).count()

print(f"\nBronze ERP rows written: {written_bronze_count}  (expected: {result.expected_bronze_row_count})")
print(f"Manifest rows written: {written_manifest_count}  (expected: {len(statement_invoices)} -- one per statement invoice)")

assert written_bronze_count == result.expected_bronze_row_count, \
    f"Bronze row count mismatch: wrote {written_bronze_count}, generator produced {result.expected_bronze_row_count}"
assert written_manifest_count == len(statement_invoices), \
    f"Manifest row count mismatch: wrote {written_manifest_count}, expected one per statement invoice ({len(statement_invoices)})"

print("\nScenario mix achieved vs. configured target:")
from collections import Counter
achieved = Counter(r["scenario_type"] for r in result.manifest_records)
total = len(statement_invoices)
for scenario_type, pct in scenarios_config["scenario_mix"].items():
    target_count = round(pct * total)
    print(f"  {scenario_type:<24} target={target_count:>4}  achieved={achieved.get(scenario_type, 0):>4}")

print("\nAll Mock ERP Generator validation checks passed.")
