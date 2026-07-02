"""
tests/test_pipeline_checks.py

Validates src/validation/pipeline_checks.py against a trivial fake `spark`
-- pipeline_checks.py never imports pyspark, so a fake only needs to
support .table(name).collect() returning plain dict rows. No pyspark
needed here at all.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.validation.pipeline_checks import (
    check_bronze_vendor_statement,
    check_silver_vendor_statement,
    check_bronze_internal_erp,
    check_silver_internal_erp,
    check_review_queue,
    check_ai_audit_log,
    check_mutation_manifest,
    run_all_checks,
)


class FakeTable:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class FakeSpark:
    def __init__(self, tables: dict):
        self._tables = tables

    def table(self, name):
        if name not in self._tables:
            raise Exception(f"Table or view not found: {name}")
        return FakeTable(self._tables[name])


def _spark(**tables):
    return FakeSpark(tables)


# ---- Bronze Vendor Statement -----------------------------------------------

def test_bronze_vendor_statement_passes_when_populated():
    spark = _spark(bronze_vendor_statement_raw=[{"raw_invoice_number": "SIN1"}, {"raw_invoice_number": "SIN2"}])
    result = check_bronze_vendor_statement(spark)
    assert result.passed is True
    assert result.row_count == 2


def test_bronze_vendor_statement_fails_when_empty():
    spark = _spark(bronze_vendor_statement_raw=[])
    result = check_bronze_vendor_statement(spark)
    assert result.passed is False
    assert result.row_count == 0


def test_bronze_vendor_statement_fails_when_table_missing():
    spark = _spark()  # no table registered at all
    result = check_bronze_vendor_statement(spark)
    assert result.passed is False
    assert "query failed" in result.details


# ---- Silver Vendor Statement ------------------------------------------------

def test_silver_vendor_statement_passes_with_all_dates_parsed():
    spark = _spark(silver_reconciliation_standard=[
        {"record_source": "VENDOR_STATEMENT", "invoice_date": "2026-05-01"},
        {"record_source": "VENDOR_STATEMENT", "invoice_date": "2026-05-02"},
        {"record_source": "INTERNAL_ERP", "invoice_date": None},  # different side -- must not affect this check
    ])
    result = check_silver_vendor_statement(spark)
    assert result.passed is True
    assert result.row_count == 2


def test_silver_vendor_statement_fails_on_unparsed_dates():
    spark = _spark(silver_reconciliation_standard=[
        {"record_source": "VENDOR_STATEMENT", "invoice_date": "2026-05-01"},
        {"record_source": "VENDOR_STATEMENT", "invoice_date": None},
    ])
    result = check_silver_vendor_statement(spark)
    assert result.passed is False
    assert "unparsed invoice_date" in result.details


def test_silver_vendor_statement_fails_when_no_matching_record_source():
    spark = _spark(silver_reconciliation_standard=[{"record_source": "INTERNAL_ERP", "invoice_date": "2026-05-01"}])
    result = check_silver_vendor_statement(spark)
    assert result.passed is False
    assert result.row_count == 0


# ---- Bronze / Silver Internal ERP ------------------------------------------

def test_bronze_internal_erp_fails_when_empty():
    spark = _spark(bronze_internal_erp_raw=[])
    result = check_bronze_internal_erp(spark)
    assert result.passed is False


def test_silver_internal_erp_passes_when_populated_and_dates_parsed():
    spark = _spark(silver_reconciliation_standard=[
        {"record_source": "INTERNAL_ERP", "invoice_date": "2026-05-01"},
    ])
    result = check_silver_internal_erp(spark)
    assert result.passed is True
    assert result.row_count == 1


# ---- Informational tables ---------------------------------------------------

def test_review_queue_passes_even_when_empty():
    spark = _spark(validation_document_review_queue=[])
    result = check_review_queue(spark)
    assert result.passed is True
    assert result.row_count == 0
    assert "informational" in result.details


def test_ai_audit_log_passes_even_when_empty():
    spark = _spark(ai_audit_log=[])
    result = check_ai_audit_log(spark)
    assert result.passed is True
    assert result.row_count == 0


def test_review_queue_fails_on_query_error_not_on_row_count():
    spark = _spark()  # table missing entirely -- a real problem, unlike a merely-empty table
    result = check_review_queue(spark)
    assert result.passed is False
    assert "query failed" in result.details


# ---- Mutation Manifest -------------------------------------------------------

def test_mutation_manifest_fails_when_empty():
    spark = _spark(validation_mutation_manifest=[])
    result = check_mutation_manifest(spark)
    assert result.passed is False


def test_mutation_manifest_passes_when_populated():
    spark = _spark(validation_mutation_manifest=[{"scenario_type": "exact_match"}] * 5)
    result = check_mutation_manifest(spark)
    assert result.passed is True
    assert result.row_count == 5


# ---- run_all_checks ----------------------------------------------------------

def test_run_all_checks_returns_seven_results_in_a_stable_order():
    spark = _spark(
        bronze_vendor_statement_raw=[{"raw_invoice_number": "SIN1"}],
        silver_reconciliation_standard=[
            {"record_source": "VENDOR_STATEMENT", "invoice_date": "2026-05-01"},
            {"record_source": "INTERNAL_ERP", "invoice_date": "2026-05-01"},
        ],
        bronze_internal_erp_raw=[{"raw_invoice_number": "SIN1"}],
        validation_document_review_queue=[],
        ai_audit_log=[],
        validation_mutation_manifest=[{"scenario_type": "exact_match"}],
    )
    results = run_all_checks(spark)
    assert len(results) == 7
    assert [r.name for r in results] == [
        "Bronze Vendor Statement", "Silver Vendor Statement", "Bronze Internal ERP",
        "Silver Internal ERP", "Review Queue", "AI Audit Log", "Mutation Manifest",
    ]
    assert all(r.passed for r in results)


def test_run_all_checks_surfaces_a_failure_without_stopping_the_rest():
    spark = _spark(
        bronze_vendor_statement_raw=[],  # fails
        silver_reconciliation_standard=[{"record_source": "VENDOR_STATEMENT", "invoice_date": "2026-05-01"}],
        bronze_internal_erp_raw=[{"raw_invoice_number": "SIN1"}],
        validation_document_review_queue=[],
        ai_audit_log=[],
        validation_mutation_manifest=[{"scenario_type": "exact_match"}],
    )
    results = run_all_checks(spark)
    by_name = {r.name: r for r in results}
    assert by_name["Bronze Vendor Statement"].passed is False
    assert by_name["Bronze Internal ERP"].passed is True  # unaffected by the other failure


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed, failed = 0, 0
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"FAIL  {t.__name__}  -- {e}")
            failed += 1
        except Exception as e:
            print(f"ERROR {t.__name__}  -- {type(e).__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
