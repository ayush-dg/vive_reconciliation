#!/usr/bin/env python3
"""
run_pipeline.py

Development/demonstration tool only -- not a Fabric deployment artifact,
not part of the tested library code (src/). Runs the currently-implemented
pipeline end to end, in one Python process, in execution order:

    00_setup_lakehouse_schema.py
    01_bronze_ingestion.py               (PDF -> Gemini Extraction -> Validation -> Bronze)
    02_silver_normalization_statement.py (Bronze -> Silver, VENDOR_STATEMENT)
    03_mock_erp_generator.py             (Silver -> Bronze ERP + manifest, no AI)
    04_silver_normalization_erp.py       (Bronze ERP -> Silver, INTERNAL_ERP)
    05_matching_engine.py                (Silver both sides -> every Gold table, no AI)

All sequencing logic lives in src/pipeline/runner.py (unit-tested without
Spark); this script is a thin CLI wrapper: parse args, wire up print-based
progress callbacks, run, print a validation report, set the exit code.

Usage
-----
    python scripts/run_pipeline.py
        Runs the full pipeline against the committed sample PDF.

    python scripts/run_pipeline.py --pdf path/to/statement.pdf \\
        [--statement-id ASTECH-DEMO-2026-06] [--statement-period 2026-06]
        Demo Mode -- runs the full pipeline against a caller-supplied PDF.
        Any identifier not given falls back to 01_bronze_ingestion.py's
        default (see that notebook's Cell 6).

    python scripts/run_pipeline.py --skip-validation
        Skips the automated post-run validation report (still runs the
        full pipeline).

Requires pyspark and pdfplumber to be installed and a Fabric-compatible
environment (or local Spark) -- see README.md's "Running the pipeline
locally" section for known local limitations (Delta/Maven Central).
"""

import argparse
import os
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)  # notebooks use paths relative to the repo root (e.g. "config/...", "sample_data/...")

from src.pipeline.runner import run_pipeline, PIPELINE_STAGES, PipelineStageFailed

DEMO_FLOW_BANNER = """
  Vendor Statement PDF
        |
        v
  Gemini Extraction    (src/ai/extraction_service.py -- pdfplumber fallback if AI fails)
        |
        v
  Validation            (src/validation/extraction_validator.py, config-driven)
        |
        v
  Bronze Vendor         (bronze_vendor_statement_raw)
        |
        v
  Silver Vendor         (silver_reconciliation_standard, record_source='VENDOR_STATEMENT')
        |
        v
  Mock ERP Generation   (src/mock_erp/generator.py -- deterministic, no AI)
        |
        v
  Bronze ERP            (bronze_internal_erp_raw)
        |
        v
  Silver ERP            (silver_reconciliation_standard, record_source='INTERNAL_ERP')
"""

RULE = "=" * 70


def _print_stage_start(stage):
    print(f"\n{RULE}\n>> {stage.name}\n{RULE}")


def _print_stage_complete(result):
    status = "OK" if result.success else "FAILED"
    print(f"\n[{status}] {result.stage.name} -- {result.elapsed_seconds:.2f}s")
    if result.error:
        print(f"    error: {result.error}")
    for table, count in result.table_counts.items():
        shown = f"{count} row(s)" if count is not None else "ERROR reading table"
        print(f"    {table}: {shown}")


def main():
    parser = argparse.ArgumentParser(description="Run the VIVE Reconciliation PoC pipeline end to end.")
    parser.add_argument("--pdf", help="Path to a vendor statement PDF -- enables Demo Mode")
    parser.add_argument("--statement-id", default=None, help="Override statement_id (Demo Mode only)")
    parser.add_argument("--statement-period", default=None, help="Override statement_period, e.g. 2026-06 (Demo Mode only)")
    parser.add_argument("--skip-validation", action="store_true", help="Skip the automated post-run validation report")
    args = parser.parse_args()

    exec_globals = {}
    demo_mode = args.pdf is not None
    if demo_mode:
        if not os.path.exists(args.pdf):
            print(f"ERROR: --pdf path does not exist: {args.pdf}")
            sys.exit(1)
        exec_globals["PDF_PATH"] = args.pdf
        exec_globals["SOURCE_FILE"] = os.path.basename(args.pdf)
        if args.statement_id:
            exec_globals["STATEMENT_ID"] = args.statement_id
        if args.statement_period:
            exec_globals["STATEMENT_PERIOD"] = args.statement_period
        print("DEMO MODE")
        print(DEMO_FLOW_BANNER)
        print(f"Input PDF: {args.pdf}")

    start_all = time.monotonic()
    try:
        results = run_pipeline(
            PIPELINE_STAGES, exec_globals,
            on_stage_start=_print_stage_start, on_stage_complete=_print_stage_complete,
        )
    except PipelineStageFailed as e:
        print(f"\n{RULE}\nPIPELINE STOPPED -- {e}\n{RULE}")
        sys.exit(1)
    total_elapsed = time.monotonic() - start_all

    print(f"\n{RULE}\nPIPELINE SUMMARY\n{RULE}")
    for r in results:
        print(f"  {r.stage.name:<52} {'OK' if r.success else 'FAILED':<8} {r.elapsed_seconds:6.2f}s")
    print(f"\nTotal elapsed: {total_elapsed:.2f}s")

    if args.skip_validation:
        sys.exit(0)

    from src.validation.pipeline_checks import run_all_checks
    spark = exec_globals.get("spark")
    checks = run_all_checks(spark)
    print(f"\n{RULE}\nVALIDATION REPORT\n{RULE}")
    for c in checks:
        print(f"  [{'PASS' if c.passed else 'FAIL'}] {c.name}: {c.details}")

    all_passed = all(c.passed for c in checks)
    print(f"\n{'All validation checks passed.' if all_passed else 'One or more validation checks FAILED -- see above.'}")
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
