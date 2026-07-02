# ==========================================================================
# Notebook: 05_matching_engine
# Purpose : Compares silver_reconciliation_standard's VENDOR_STATEMENT rows
#           against its INTERNAL_ERP rows and populates every Gold table.
#           100% deterministic -- AI does not participate in any matching
#           decision. Graded automatically against
#           validation_mutation_manifest's known-planted scenarios.
#
# Design notes:
#   - The actual decision logic (every matching level, every exception
#     category) lives in ONE place: src/matching/engine.py::classify_match().
#     It's a pure Python function, exhaustively unit-tested directly
#     (tests/test_matching_engine.py), and this notebook calls the exact
#     same function via a Spark UDF -- not a hand-transliterated Spark
#     equivalent that could drift from its tests. Mirrors
#     src/normalization.py's normalize_invoice_number()/make_spark_udf()
#     pattern already established in this repo.
#   - Duplicate detection and the Level 4 work-order lookup are prepared as
#     plain Spark joins/groupBy (no rule invented here -- they only feed
#     classify_match() the exact inputs it's documented to need); the UDF
#     itself is the only per-row Python code, and it runs distributed
#     across executors, not as a driver-side loop.
#   - Matching hierarchy (see src/matching/engine.py's docstring for the
#     full decision tree): Level 1 joins on invoice_number_normalized
#     directly, so invoice_revision resolves at Level 1 (revision suffixes
#     are already stripped by Silver normalization before this notebook
#     ever runs) -- Levels 2/3 are real, tested rules for RO-missing /
#     RO-conflict cases that the current Mock ERP scenario mix doesn't
#     happen to produce (no mutator sets ro_number to a different
#     non-null value). Level 4 never produces a match; it only enriches
#     an Invoice Missing exception's deterministic_reason when the ERP
#     side's invoice_number equals the statement's work_order_number.
#   - gold_vendor_summary, gold_shop_summary, and gold_reconciliation_summary
#     are ALL derived from one per-shop aggregation (not computed three
#     separate times), so their totals can never silently disagree with
#     each other.
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
from src.matching.engine import classify_match
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ---- CELL 2: Load config --------------------------------------------------
with open("config/vendors/astech.json") as f:
    vendor_config = json.load(f)

with open("config/matching/astech_matching_rules.json") as f:
    matching_rules = json.load(f)

VENDOR_ID = vendor_config["vendor_id"]
STATEMENT_PERIOD = vendor_config["period"]["end_date"][:7]
AMOUNT_TOLERANCE = matching_rules["amount_comparison_tolerance"]
MINOR_VARIANCE_THRESHOLD_PCT = matching_rules["minor_variance_threshold_pct"]

print(f"Matching Engine: {vendor_config['vendor_name']} ({VENDOR_ID}), period {STATEMENT_PERIOD}")
print(f"amount_comparison_tolerance={AMOUNT_TOLERANCE}, minor_variance_threshold_pct={MINOR_VARIANCE_THRESHOLD_PCT}")

# ---- CELL 3: Read Silver, split by side -----------------------------------
silver = spark.table("silver_reconciliation_standard").filter(
    (F.col("vendor_id") == VENDOR_ID) & (F.col("statement_period") == STATEMENT_PERIOD)
)
stmt = silver.filter(F.col("record_source") == "VENDOR_STATEMENT")
erp = silver.filter(F.col("record_source") == "INTERNAL_ERP")

stmt_count = stmt.count()
erp_count = erp.count()
print(f"Read {stmt_count} VENDOR_STATEMENT rows, {erp_count} INTERNAL_ERP rows from Silver")

# ---- CELL 4: ERP candidates grouped by normalized invoice number ---------
# One row per (vendor_id, invoice_number_normalized), erp_candidates is the
# list of EVERY ERP row sharing that key -- 0 (absent from the grouped
# result entirely), 1 (the normal case), or 2+ (duplicate_invoice).
erp_candidate_struct = F.struct(
    F.col("record_id"), F.col("invoice_number"), F.col("outstanding_amount"),
    F.col("amount"), F.col("ro_number"), F.col("status"), F.col("credit"),
).alias("candidate")

erp_grouped = (
    erp.select("vendor_id", "invoice_number_normalized", erp_candidate_struct)
    .groupBy("vendor_id", "invoice_number_normalized")
    .agg(F.collect_list("candidate").alias("erp_candidates"))
)

# ---- CELL 5: Level 4 input -- work-order lookup (enrichment only) --------
# For statement invoices with NO candidate above, does any ERP row's
# invoice_number equal THIS statement invoice's work_order_number? This
# never produces a match (see engine.py's docstring / the confirmed
# ground truth for vendor_reference_issue) -- it only lets the eventual
# Invoice Missing exception explain itself more precisely.
erp_invoice_numbers = (
    erp.select(F.col("vendor_id").alias("wo_vendor_id"), F.col("invoice_number").alias("wo_erp_invoice_number"))
    .dropDuplicates(["wo_vendor_id", "wo_erp_invoice_number"])
)

stmt_with_wo_match = (
    stmt.join(
        erp_invoice_numbers,
        (stmt["vendor_id"] == erp_invoice_numbers["wo_vendor_id"]) &
        (stmt["work_order_number"] == erp_invoice_numbers["wo_erp_invoice_number"]),
        "left",
    )
    .select(stmt["*"], F.col("wo_erp_invoice_number").alias("work_order_match_invoice_number"))
)

# ---- CELL 6: Join candidates onto each statement invoice ------------------
joined = (
    stmt_with_wo_match
    .join(erp_grouped, on=["vendor_id", "invoice_number_normalized"], how="left")
    .withColumn("erp_candidates", F.coalesce(F.col("erp_candidates"), F.array()))
)

# ---- CELL 7: Apply classify_match() via a Spark UDF -----------------------
# One implementation, not a Spark transliteration of a tested Python
# function -- this UDF calls src/matching/engine.py::classify_match()
# directly, exactly like src/normalization.py's make_spark_udf() wraps
# normalize_invoice_number().
decision_schema = StructType([
    StructField("match_status", StringType()),
    StructField("match_level", IntegerType()),
    StructField("matched_rule", StringType()),
    StructField("match_reason", StringType()),
    StructField("exception_category", StringType()),
    StructField("deterministic_reason", StringType()),
    StructField("matched_erp_record_id", StringType()),
])


def _make_classify_udf(tolerance):
    from pyspark.sql.functions import udf

    @udf(returnType=decision_schema)
    def _classify(stmt_struct, erp_candidates, work_order_match_invoice_number):
        stmt_dict = stmt_struct.asDict()
        candidates = [c.asDict() for c in (erp_candidates or [])]
        decision = classify_match(stmt_dict, candidates, work_order_match_invoice_number, tolerance=tolerance)
        return (
            decision.match_status, decision.match_level, decision.matched_rule, decision.match_reason,
            decision.exception_category, decision.deterministic_reason, decision.matched_erp_record_id,
        )

    return _classify


classify_udf = _make_classify_udf(AMOUNT_TOLERANCE)

stmt_struct_col = F.struct(
    F.col("invoice_number"), F.col("invoice_number_normalized"), F.col("outstanding_amount"),
    F.col("ro_number"), F.col("work_order_number"), F.col("record_id"),
).alias("stmt_struct")

classified = joined.withColumn(
    "decision", classify_udf(stmt_struct_col, F.col("erp_candidates"), F.col("work_order_match_invoice_number"))
)

matched_df = classified.filter(F.col("decision.match_status") == "MATCHED")
exception_df = classified.filter(F.col("decision.match_status") == "EXCEPTION")

print(f"Classified {classified.count()} statement invoices: "
      f"{matched_df.count()} matched, {exception_df.count()} flagged as exceptions.")

# ---- CELL 8: Build gold_matched_invoices / gold_exceptions rows ----------
gold_matched = matched_df.select(
    F.expr("uuid()").alias("match_id"),
    F.col("vendor_id"), F.col("shop"), F.col("invoice_number"), F.col("ro_number"),
    F.col("outstanding_amount").alias("amount"),
    F.col("decision.match_level").alias("match_level"),
    F.col("decision.matched_rule").alias("matched_rule"),
    F.col("decision.match_reason").alias("match_reason"),
    F.lit("MATCHED").alias("match_status"),
    F.col("record_id").alias("statement_record_id"),
    F.col("decision.matched_erp_record_id").alias("reference_record_id"),
    F.col("source_file"), F.col("statement_id"),
    F.current_timestamp().alias("match_timestamp"),
    F.col("statement_period"),
)

gold_exceptions_from_stmt = exception_df.select(
    F.expr("uuid()").alias("exception_id"),
    F.col("vendor_id"), F.col("shop"), F.col("invoice_number"), F.col("ro_number"),
    F.col("outstanding_amount").alias("amount"),
    F.lit("EXCEPTION").alias("match_status"),
    F.col("decision.exception_category").alias("exception_category"),
    F.col("decision.deterministic_reason").alias("deterministic_reason"),
    F.col("decision.matched_erp_record_id").alias("reference_record_id"),
    F.lit("Open").alias("exception_status"),
    F.col("record_id").alias("statement_record_id"),
    F.col("source_file"), F.col("statement_id"),
    F.current_timestamp().alias("date_raised"),
    F.lit(None).cast("timestamp").alias("date_resolved"),
    F.col("statement_period"),
)

# Orphaned ERP rows -- an ERP transaction with NO statement counterpart at
# all. None of the 8 current Mock ERP scenarios produce one (the generator
# always seeds from a statement invoice), but a real NetSuite feed could,
# so this is implemented and graded like everything else, not skipped.
#
# Second anti-join is required, not optional: without it, a
# vendor_reference_issue ERP row (invoice_number = some statement invoice's
# work_order_number) would ALSO fail the first anti-join's normalized-number
# test and get double-counted here as ANOTHER exception on top of that same
# statement invoice's own "Invoice Missing" -- one real discrepancy, two
# gold_exceptions rows. Excluding any ERP row whose invoice_number equals
# some statement invoice's work_order_number avoids that double-count.
work_order_numbers = (
    stmt.select(F.col("vendor_id").alias("wo_vendor_id2"), F.col("work_order_number").alias("wo_number2"))
    .filter(F.col("wo_number2").isNotNull())
    .dropDuplicates()
)
orphan_erp = (
    erp.join(stmt, ["vendor_id", "invoice_number_normalized"], "left_anti")
    .join(
        work_order_numbers,
        (F.col("vendor_id") == F.col("wo_vendor_id2")) & (F.col("invoice_number") == F.col("wo_number2")),
        "left_anti",
    )
)

gold_exceptions_from_orphans = orphan_erp.select(
    F.expr("uuid()").alias("exception_id"),
    F.col("vendor_id"), F.col("shop"), F.col("invoice_number"), F.col("ro_number"),
    F.col("outstanding_amount").alias("amount"),
    F.lit("EXCEPTION").alias("match_status"),
    F.lit("Unmatched Record").alias("exception_category"),
    F.concat(F.lit("ERP record for invoice "), F.col("invoice_number"),
              F.lit(" has no corresponding Vendor Statement invoice.")).alias("deterministic_reason"),
    F.col("record_id").alias("reference_record_id"),
    F.lit("Open").alias("exception_status"),
    F.lit(None).cast("string").alias("statement_record_id"),
    F.col("source_file"), F.col("statement_id"),
    F.current_timestamp().alias("date_raised"),
    F.lit(None).cast("timestamp").alias("date_resolved"),
    F.col("statement_period"),
)

gold_exceptions_all = gold_exceptions_from_stmt.unionByName(gold_exceptions_from_orphans)

print(f"Orphaned ERP records (no statement counterpart): {orphan_erp.count()}")

# ---- CELL 9: Idempotent writes -- gold_matched_invoices, gold_exceptions -
for table_name, df in [("gold_matched_invoices", gold_matched), ("gold_exceptions", gold_exceptions_all)]:
    row_count = df.count()  # captured before the write so printing it doesn't force a second computation of df
    try:
        spark.sql(f"DELETE FROM {table_name} WHERE vendor_id = '{VENDOR_ID}' AND statement_period = '{STATEMENT_PERIOD}'")
    except Exception as e:
        print(f"(DELETE skipped on {table_name} -- expected on non-Delta local test tables: {e})")
    df.write.mode("append").saveAsTable(table_name)
    print(f"{table_name} written: {row_count} row(s).")

# ---- CELL 10: Per-shop aggregation -- feeds ALL three summary tables -----
# Computed exactly once so vendor/shop/reconciliation totals can never
# silently disagree with each other (a risk flagged explicitly during
# design review).
stmt_shop_agg = stmt.groupBy("vendor_id", "shop").agg(
    F.count("*").alias("total_invoice_count"),
    F.sum("outstanding_amount").alias("statement_total"),
)
matched_shop_agg = matched_df.groupBy("vendor_id", "shop").agg(F.count("*").alias("matched_count"))
exception_shop_agg = gold_exceptions_all.groupBy("vendor_id", "shop").agg(F.count("*").alias("exception_count"))
erp_shop_agg = erp.groupBy("vendor_id", "shop").agg(F.sum("outstanding_amount").alias("internal_erp_total"))

recon = (
    stmt_shop_agg
    .join(matched_shop_agg, ["vendor_id", "shop"], "left")
    .join(exception_shop_agg, ["vendor_id", "shop"], "left")
    .join(erp_shop_agg, ["vendor_id", "shop"], "left")
    .withColumn("matched_count", F.coalesce(F.col("matched_count"), F.lit(0)))
    .withColumn("exception_count", F.coalesce(F.col("exception_count"), F.lit(0)))
    .withColumn("internal_erp_total", F.coalesce(F.col("internal_erp_total"), F.lit(0.0)))
    .withColumn("vendor_name", F.lit(vendor_config["vendor_name"]))
    .withColumn("statement_period", F.lit(STATEMENT_PERIOD))
    .withColumn("difference", F.col("statement_total") - F.col("internal_erp_total"))
    .withColumn("match_pct", F.round(F.col("matched_count") / F.col("total_invoice_count") * 100, 2))
    .withColumn(
        "variance_pct",
        F.when(F.col("statement_total") != 0, F.abs(F.col("difference")) / F.col("statement_total")).otherwise(F.lit(0.0)),
    )
    .withColumn(
        "overall_status",
        F.when(F.col("exception_count") > 0, F.lit("EXCEPTIONS_PRESENT"))
         .when(F.col("variance_pct") > MINOR_VARIANCE_THRESHOLD_PCT, F.lit("MINOR_VARIANCE"))
         .otherwise(F.lit("RECONCILED")),
    )
)

gold_shop_summary_df = recon.select(
    "shop", "vendor_id", "statement_period",
    F.col("total_invoice_count").alias("total_invoices"),
    "matched_count", "exception_count", "match_pct",
)

gold_reconciliation_summary_df = recon.select(
    "vendor_id", "vendor_name", "shop", "statement_period", "statement_total", "internal_erp_total",
    "difference", "total_invoice_count", "matched_count", "exception_count", "match_pct", "overall_status",
)

gold_vendor_summary_df = (
    recon.groupBy("vendor_id", "vendor_name", "statement_period")
    .agg(
        F.sum("total_invoice_count").alias("total_invoices"),
        F.sum("matched_count").alias("matched_count"),
        F.sum("exception_count").alias("exception_count"),
        F.sum("statement_total").alias("total_outstanding_amount"),
    )
    .withColumn("match_pct", F.round(F.col("matched_count") / F.col("total_invoices") * 100, 2))
)

for table_name, df in [
    ("gold_vendor_summary", gold_vendor_summary_df),
    ("gold_shop_summary", gold_shop_summary_df),
    ("gold_reconciliation_summary", gold_reconciliation_summary_df),
]:
    row_count = df.count()
    try:
        spark.sql(f"DELETE FROM {table_name} WHERE vendor_id = '{VENDOR_ID}' AND statement_period = '{STATEMENT_PERIOD}'")
    except Exception as e:
        print(f"(DELETE skipped on {table_name} -- expected on non-Delta local test tables: {e})")
    df.write.mode("append").saveAsTable(table_name)
    print(f"{table_name} written: {row_count} row(s).")

# ---- CELL 11: Grade against validation_mutation_manifest ------------------
# This is exactly what that table exists for: known-planted scenarios with
# a known-correct expected outcome, letting "does the engine work" be a
# hard yes/no instead of "the output looks plausible."
manifest = spark.table("validation_mutation_manifest").filter(
    (F.col("vendor_id") == VENDOR_ID) & (F.col("statement_period") == STATEMENT_PERIOD)
)

actuals = classified.select(
    F.col("invoice_number").alias("stmt_invoice_number"),
    F.col("decision.match_status").alias("actual_match_status"),
    F.col("decision.match_level").alias("actual_match_level"),
    F.col("decision.exception_category").alias("actual_exception_category"),
)

graded = (
    manifest.join(actuals, manifest["statement_invoice_number"] == actuals["stmt_invoice_number"], "left")
    .withColumn("status_correct", F.col("expected_match_status") == F.col("actual_match_status"))
    .withColumn(
        "detail_correct",
        F.when(F.col("expected_match_status") == "MATCHED", F.col("expected_match_level") == F.col("actual_match_level"))
         .otherwise(F.col("expected_exception_reason") == F.col("actual_exception_category")),
    )
    .withColumn("graded_correct", F.col("status_correct") & F.col("detail_correct"))
)

total_graded = graded.count()
correct_graded = graded.filter(F.col("graded_correct")).count()
print(f"\nGraded against validation_mutation_manifest: {correct_graded}/{total_graded} statement invoices "
      f"classified exactly as expected.")

print("\nPer-scenario breakdown:")
breakdown = (
    graded.groupBy("scenario_type")
    .agg(F.count("*").alias("total"), F.sum(F.col("graded_correct").cast("int")).alias("correct"))
    .collect()
)
for row in breakdown:
    flag = "OK" if row["correct"] == row["total"] else "MISMATCH"
    print(f"  [{flag}] {row['scenario_type']:<24} {row['correct']}/{row['total']}")

mismatches = graded.filter(~F.col("graded_correct")).select(
    "scenario_type", "statement_invoice_number", "expected_match_status", "actual_match_status",
    "expected_match_level", "actual_match_level", "expected_exception_reason", "actual_exception_category",
)
if correct_graded != total_graded:
    print("\nMismatches:")
    for row in mismatches.collect():
        print(f"  {row.asDict()}")

assert correct_graded == total_graded, (
    f"Matching Engine disagrees with validation_mutation_manifest on "
    f"{total_graded - correct_graded} of {total_graded} invoice(s) -- see breakdown above."
)

print("\nAll Matching Engine validation checks passed.")
