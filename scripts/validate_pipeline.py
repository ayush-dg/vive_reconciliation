#!/usr/bin/env python3
"""
validate_pipeline.py

Development/demonstration tool only. Checks whatever is CURRENTLY in the
lakehouse against src/validation/pipeline_checks.py -- does not re-run any
notebook. Use this after a manual Fabric run, or any time you want to
re-check the current state without paying the cost of re-running the whole
pipeline (e.g. after inspecting/editing a table by hand).

Usage
-----
    python scripts/validate_pipeline.py

Requires pyspark and an existing lakehouse session/warehouse with the
tables already created (run notebooks/00_setup_lakehouse_schema.py at
least once first).
"""

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)

from src.validation.pipeline_checks import run_all_checks

RULE = "=" * 70


def _build_spark():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("VIVE_Reconciliation_PoC_Validate")
        .config("spark.sql.warehouse.dir", "/home/claude/vive_reconciliation_poc/lakehouse")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = _build_spark()
    checks = run_all_checks(spark)

    print(f"{RULE}\nVALIDATION REPORT\n{RULE}")
    for c in checks:
        print(f"  [{'PASS' if c.passed else 'FAIL'}] {c.name}: {c.details}")

    all_passed = all(c.passed for c in checks)
    print(f"\n{'All validation checks passed.' if all_passed else 'One or more validation checks FAILED -- see above.'}")
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
