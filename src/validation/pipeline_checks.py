"""
pipeline_checks.py

Purpose
-------
Structural, automated checks confirming each pipeline stage's output table
is populated as expected -- used by scripts/run_pipeline.py (automatically,
right after a full run) and scripts/validate_pipeline.py (standalone,
against whatever is currently in the lakehouse).

Deliberately never imports pyspark. Every check calls
spark.table(name).collect() and then filters/aggregates in plain Python --
this works against a real SparkSession (.collect() returns dict-like Row
objects) while keeping this module Spark-free at import time, consistent
with every other src/ module in this repo, and trivially testable with a
fake `spark` duck-type that has nothing to do with pyspark. Pulling a
whole table into the driver is fine at this PoC's scale (hundreds of
rows) for a development/demonstration validation utility -- this is not
meant to run against a production-sized table.

What "populated correctly" means here, per table
--------------------------------------------------
- Bronze Vendor / Bronze ERP: at least one row exists.
- Silver Vendor / Silver ERP: at least one row exists for that
  record_source, AND invoice_date parsed for every one of them (a null
  invoice_date in Silver is always a genuine normalization bug, never an
  expected state).
- Review Queue / AI Audit Log: informational only -- zero rows can be
  entirely correct (a clean AI run flags nothing; a pdfplumber-only run
  never calls the AI at all). These checks report the count and pass as
  long as the table is queryable.
- Mutation Manifest: at least one row exists -- the Mock ERP Generator
  always writes exactly one manifest row per statement invoice it read,
  so zero rows here means the generator did not actually run.
- Gold Matched Invoices / Gold Exceptions: at least one row exists (the
  current scenario mix always produces both some matches and some
  exceptions), and every row has its explainability fields populated --
  matched_rule/match_reason for matches, exception_category/
  deterministic_reason for exceptions -- since a NULL there means the
  Matching Engine produced a row without explaining itself.
- Gold Vendor / Shop / Reconciliation Summary: at least one row exists;
  Reconciliation Summary's overall_status must be one of the three values
  the Matching Engine ever assigns.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str
    row_count: Optional[int] = None


def _rows(spark, table_name):
    """Returns (list_of_dicts, error_message). error_message is None on success."""
    try:
        collected = spark.table(table_name).collect()
    except Exception as e:
        return None, str(e)
    rows = [r.asDict() if hasattr(r, "asDict") else dict(r) for r in collected]
    return rows, None


def check_bronze_vendor_statement(spark) -> CheckResult:
    rows, error = _rows(spark, "bronze_vendor_statement_raw")
    if error:
        return CheckResult("Bronze Vendor Statement", False, f"query failed: {error}")
    if not rows:
        return CheckResult("Bronze Vendor Statement", False, "table is empty", 0)
    return CheckResult("Bronze Vendor Statement", True, f"{len(rows)} row(s)", len(rows))


def check_silver_vendor_statement(spark) -> CheckResult:
    return _check_silver_side(spark, "VENDOR_STATEMENT", "Silver Vendor Statement")


def check_bronze_internal_erp(spark) -> CheckResult:
    rows, error = _rows(spark, "bronze_internal_erp_raw")
    if error:
        return CheckResult("Bronze Internal ERP", False, f"query failed: {error}")
    if not rows:
        return CheckResult("Bronze Internal ERP", False, "table is empty -- run 03_mock_erp_generator.py", 0)
    return CheckResult("Bronze Internal ERP", True, f"{len(rows)} row(s)", len(rows))


def check_silver_internal_erp(spark) -> CheckResult:
    return _check_silver_side(spark, "INTERNAL_ERP", "Silver Internal ERP")


def _check_silver_side(spark, record_source: str, display_name: str) -> CheckResult:
    rows, error = _rows(spark, "silver_reconciliation_standard")
    if error:
        return CheckResult(display_name, False, f"query failed: {error}", None)
    side_rows = [r for r in rows if r.get("record_source") == record_source]
    if not side_rows:
        return CheckResult(display_name, False, f"no {record_source} rows found", 0)
    null_dates = sum(1 for r in side_rows if r.get("invoice_date") is None)
    passed = null_dates == 0
    details = f"{len(side_rows)} row(s)"
    if not passed:
        details += f", {null_dates} with unparsed invoice_date"
    return CheckResult(display_name, passed, details, len(side_rows))


def check_review_queue(spark) -> CheckResult:
    rows, error = _rows(spark, "validation_document_review_queue")
    if error:
        return CheckResult("Review Queue", False, f"query failed: {error}")
    return CheckResult("Review Queue", True, f"{len(rows)} row(s) flagged for review (informational)", len(rows))


def check_ai_audit_log(spark) -> CheckResult:
    rows, error = _rows(spark, "ai_audit_log")
    if error:
        return CheckResult("AI Audit Log", False, f"query failed: {error}")
    return CheckResult("AI Audit Log", True, f"{len(rows)} AI call(s) logged (informational)", len(rows))


def check_mutation_manifest(spark) -> CheckResult:
    rows, error = _rows(spark, "validation_mutation_manifest")
    if error:
        return CheckResult("Mutation Manifest", False, f"query failed: {error}")
    if not rows:
        return CheckResult("Mutation Manifest", False, "table is empty -- run 03_mock_erp_generator.py", 0)
    return CheckResult("Mutation Manifest", True, f"{len(rows)} row(s)", len(rows))


def check_gold_matched_invoices(spark) -> CheckResult:
    rows, error = _rows(spark, "gold_matched_invoices")
    if error:
        return CheckResult("Gold Matched Invoices", False, f"query failed: {error}")
    if not rows:
        return CheckResult("Gold Matched Invoices", False, "table is empty -- run 05_matching_engine.py", 0)
    unexplained = sum(1 for r in rows if r.get("matched_rule") is None or r.get("match_reason") is None)
    passed = unexplained == 0
    details = f"{len(rows)} row(s)"
    if not passed:
        details += f", {unexplained} missing matched_rule/match_reason"
    return CheckResult("Gold Matched Invoices", passed, details, len(rows))


def check_gold_exceptions(spark) -> CheckResult:
    rows, error = _rows(spark, "gold_exceptions")
    if error:
        return CheckResult("Gold Exceptions", False, f"query failed: {error}")
    if not rows:
        return CheckResult("Gold Exceptions", False, "table is empty -- run 05_matching_engine.py", 0)
    unexplained = sum(1 for r in rows if r.get("exception_category") is None or r.get("deterministic_reason") is None)
    passed = unexplained == 0
    details = f"{len(rows)} row(s)"
    if not passed:
        details += f", {unexplained} missing exception_category/deterministic_reason"
    return CheckResult("Gold Exceptions", passed, details, len(rows))


def check_gold_vendor_summary(spark) -> CheckResult:
    rows, error = _rows(spark, "gold_vendor_summary")
    if error:
        return CheckResult("Gold Vendor Summary", False, f"query failed: {error}")
    if not rows:
        return CheckResult("Gold Vendor Summary", False, "table is empty -- run 05_matching_engine.py", 0)
    return CheckResult("Gold Vendor Summary", True, f"{len(rows)} row(s)", len(rows))


def check_gold_shop_summary(spark) -> CheckResult:
    rows, error = _rows(spark, "gold_shop_summary")
    if error:
        return CheckResult("Gold Shop Summary", False, f"query failed: {error}")
    if not rows:
        return CheckResult("Gold Shop Summary", False, "table is empty -- run 05_matching_engine.py", 0)
    return CheckResult("Gold Shop Summary", True, f"{len(rows)} row(s)", len(rows))


VALID_OVERALL_STATUSES = {"RECONCILED", "MINOR_VARIANCE", "EXCEPTIONS_PRESENT"}


def check_gold_reconciliation_summary(spark) -> CheckResult:
    rows, error = _rows(spark, "gold_reconciliation_summary")
    if error:
        return CheckResult("Gold Reconciliation Summary", False, f"query failed: {error}")
    if not rows:
        return CheckResult("Gold Reconciliation Summary", False, "table is empty -- run 05_matching_engine.py", 0)
    invalid_status = sum(1 for r in rows if r.get("overall_status") not in VALID_OVERALL_STATUSES)
    passed = invalid_status == 0
    details = f"{len(rows)} row(s)"
    if not passed:
        details += f", {invalid_status} with an unrecognized overall_status"
    return CheckResult("Gold Reconciliation Summary", passed, details, len(rows))


def run_all_checks(spark) -> list:
    return [
        check_bronze_vendor_statement(spark),
        check_silver_vendor_statement(spark),
        check_bronze_internal_erp(spark),
        check_silver_internal_erp(spark),
        check_review_queue(spark),
        check_ai_audit_log(spark),
        check_mutation_manifest(spark),
        check_gold_matched_invoices(spark),
        check_gold_exceptions(spark),
        check_gold_vendor_summary(spark),
        check_gold_shop_summary(spark),
        check_gold_reconciliation_summary(spark),
    ]
