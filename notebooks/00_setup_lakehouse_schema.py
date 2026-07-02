# ==========================================================================
# Notebook: 00_setup_lakehouse_schema
# Purpose : Creates every Bronze / Silver / Gold Delta table the PoC needs,
#           empty, with the schema locked in up front.
# Run in  : Microsoft Fabric, attached to the VIVE_Reconciliation_LH Lakehouse.
# Scope   : One-time setup. Re-running is safe (CREATE TABLE IF NOT EXISTS).
#
# v2 changes (post-architecture-review):
#   - Split Bronze into TWO raw tables: bronze_vendor_statement_raw and
#     bronze_internal_erp_raw. They have different raw column shapes
#     (a statement PDF and a Payment Voucher PDF don't look alike), so
#     Bronze -- which mirrors the source -- must not force them into one.
#   - Silver gains statement_id, statement_date, document_type, and
#     invoice_number_normalized.
#   - record_source values are now source-agnostic: 'VENDOR_STATEMENT'
#     and 'INTERNAL_ERP' -- the matching engine reads only this field
#     and never document_type, which is what makes the NetSuite swap
#     later a zero-code-change adapter swap.
#   - Gold tables carry source_file/statement_id directly for audit
#     convenience (denormalized lineage, on top of the FK-based lineage
#     back to Silver).
#   - New gold_reconciliation_summary table for management reporting.
#   - page_number / row_number added to both Bronze tables for
#     extraction-level traceability.
#
# Matching Engine phase changes:
#   - gold_matched_invoices gains matched_rule and match_reason (additive,
#     nullable) -- every matched record now explains which rule fired and
#     why, per src/matching/engine.py.
#   - gold_exceptions' exception_reason is renamed exception_category (Gold
#     was never populated before this phase, so this is a clean rename,
#     not a breaking schema change) and gains deterministic_reason
#     (detailed explanation) and reference_record_id (closest ERP
#     candidate, when one exists).
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

# ---- CELL 2: Bronze -- raw vendor statement lines -----------------------
# Mirrors exactly what came out of a Vendor Statement PDF. Every business
# field stays STRING -- nothing is trusted or parsed yet.
#
# v3 addition (AI-enabled architecture): five nullable columns capturing
# what an AI extraction provider (Gemini today) can supply beyond plain
# text -- confidence, model, section, and location metadata. All NULL
# when the legacy pdfplumber path populates this table instead; nothing
# downstream breaks either way.
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_vendor_statement_raw (
    vendor_id               STRING,    -- which vendor config drove this extraction, e.g. 'ASTECH'
    source_file             STRING,    -- original PDF filename -- lineage anchor #1
    statement_id            STRING,    -- business key for this specific statement document
    statement_period        STRING,    -- e.g. '2026-05'
    page_number             INT,       -- which PDF page this line came from -- AI-provided when available, pdfplumber-provided otherwise
    row_number               INT,       -- row position within that page, extraction order
    ingestion_timestamp     TIMESTAMP, -- when this row was extracted -- lineage anchor #2
    raw_invoice_date        STRING,
    raw_invoice_number      STRING,
    raw_work_order_number   STRING,
    raw_ro_number            STRING,
    raw_outstanding_amount  STRING,
    raw_due_date            STRING,
    raw_shop_name           STRING,
    extraction_confidence   DECIMAL(5,4), -- 0.0000-1.0000 -- NULL if the provider (or fallback parser) doesn't supply one
    extraction_model        STRING,       -- e.g. 'gemini-2.0-flash' -- NULL for pdfplumber-sourced rows
    document_section        STRING,       -- e.g. 'Outstanding Invoices' -- semantic section label, when the provider can identify one
    bounding_box             STRING,       -- raw JSON string of location/bounding info, provider-format-agnostic -- NULL if unavailable
    raw_gemini_response      STRING        -- full raw JSON text for this document's extraction batch, kept for audit -- NULL for pdfplumber-sourced rows
)
USING DELTA
""")

# ---- CELL 3: Bronze -- raw Internal ERP lines ---------------------------
# Mirrors exactly what came out of whichever system is currently playing
# the "Internal ERP Dataset" role. Today that's the Mock ERP Generator
# (see config/erp/internal_erp.json, active_adapter = "mock_erp_generator"),
# which produces fields shaped like a real ERP invoice extract -- not a
# Payment Voucher's free-text description line. This is deliberate: it
# means this table's shape already looks like what a NetSuite pull would
# look like, so swapping the adapter later changes nothing structural.
#
# page_number is nullable here -- a generated record has no PDF page.
# row_number is repurposed as "generation sequence" for generated data,
# keeping one consistent lineage column across both Bronze tables rather
# than inventing a parallel column that only generated data would use.
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_internal_erp_raw (
    vendor_id                STRING,
    source_file              STRING,   -- e.g. 'mock_erp_generator_v1.0' when generated; a real filename once NetSuite lands
    statement_id             STRING,   -- business key for this ERP extract/batch
    statement_period         STRING,
    page_number              INT,      -- NULL for generated data
    row_number                INT,      -- generation sequence for generated data
    ingestion_timestamp      TIMESTAMP,
    raw_vendor               STRING,
    raw_invoice_number       STRING,
    raw_invoice_date         STRING,
    raw_posting_date         STRING,   -- when the ERP actually posted the transaction -- distinct from invoice_date
    raw_amount               STRING,
    raw_outstanding_amount   STRING,
    raw_ro_number             STRING,
    raw_po_number             STRING,
    raw_shop                 STRING,
    raw_status                STRING    -- e.g. 'POSTED', 'PENDING'
)
USING DELTA
""")

# ---- CELL 4: Silver -- one shared, standard schema ----------------------
# BOTH sides of the reconciliation -- vendor statement and Internal ERP --
# land in this ONE table shape, distinguished by record_source. The
# matching engine (Phase 4) joins this table to itself on that column
# and never needs to know anything about document_type.
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_reconciliation_standard (
    record_id                 STRING,        -- surrogate key: hash of source+vendor+invoice+amount
    record_source             STRING,        -- 'VENDOR_STATEMENT' or 'INTERNAL_ERP' -- matching engine's ONLY filter
    document_type             STRING,        -- 'VENDOR_STATEMENT' | 'PAYMENT_VOUCHER' | (later) 'NETSUITE_INVOICE' -- audit only
    statement_id               STRING,        -- business key of the source document
    statement_date            DATE,          -- date printed on that document
    vendor_id                 STRING,
    vendor_name               STRING,
    shop                      STRING,
    invoice_number            STRING,        -- as extracted, unmodified
    invoice_number_normalized STRING,        -- revision suffixes stripped -- see src/normalization.py
    invoice_date              DATE,
    ro_number                 STRING,
    work_order_number         STRING,        -- statement-side concept, nullable -- captured in Bronze but previously dropped before Silver; the Mock ERP Generator's vendor_reference_issue scenario needs the real value
    po_number                 STRING,
    amount                    DECIMAL(12,2),
    credit                    DECIMAL(12,2),
    outstanding_amount        DECIMAL(12,2),
    due_date                  DATE,
    posting_date              DATE,          -- ERP-side concept: when the transaction actually posted -- NULL for statement-side rows
    status                    STRING,        -- ERP-side concept: e.g. 'POSTED', 'PENDING' -- NULL for statement-side rows
    description               STRING,
    statement_period          STRING,
    source_file               STRING,        -- lineage anchor, carried through from Bronze
    ingestion_timestamp       TIMESTAMP
)
USING DELTA
""")

# ---- CELL 5: Gold -- matched invoices ------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_matched_invoices (
    match_id             STRING,
    vendor_id            STRING,
    shop                 STRING,
    invoice_number       STRING,
    ro_number            STRING,
    amount               DECIMAL(12,2),
    match_level          INT,       -- 1, 2, or 3
    matched_rule         STRING,    -- Matching Engine: short code, e.g. 'LEVEL_1_FULL_MATCH' -- see src/matching/engine.py
    match_reason         STRING,    -- Matching Engine: human-readable explanation of why this pair matched
    match_status         STRING,    -- always 'MATCHED'
    statement_record_id  STRING,    -- FK -> silver_reconciliation_standard (VENDOR_STATEMENT side)
    reference_record_id  STRING,    -- FK -> silver_reconciliation_standard (INTERNAL_ERP side)
    source_file           STRING,    -- denormalized lineage -- statement-side source document
    statement_id          STRING,    -- denormalized lineage -- statement-side business key
    match_timestamp       TIMESTAMP,
    statement_period      STRING
)
USING DELTA
""")

# ---- CELL 6: Gold -- exceptions ------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_exceptions (
    exception_id         STRING,
    vendor_id            STRING,
    shop                 STRING,
    invoice_number       STRING,
    ro_number            STRING,
    amount               DECIMAL(12,2),
    match_status         STRING,    -- always 'EXCEPTION'
    exception_category   STRING,    -- Matching Engine: 'Invoice Missing' | 'Amount Mismatch' | 'Duplicate Invoice' | 'Missing Credit' | 'Pending Posting' | 'Unmatched Record' -- matches config/mock_erp/astech_scenarios.json's expected_exception_reason vocabulary exactly, so grading against validation_mutation_manifest is a literal string comparison. Renamed from exception_reason (Gold was never populated before this phase, so this is a clean rename, not a breaking change).
    deterministic_reason STRING,    -- Matching Engine: human-readable explanation of why this specific record fell into that category -- never AI-generated (see src/matching/engine.py); AI-generated narrative comes later, in a different column, in a future phase
    reference_record_id  STRING,    -- FK -> silver_reconciliation_standard (INTERNAL_ERP side) -- the closest ERP candidate this exception is based on, when exactly one exists; NULL for Duplicate Invoice (ambiguous which copy), Invoice Missing, and Unmatched Record (no statement counterpart)
    exception_status     STRING,    -- Open -> Investigating -> Resolved -> Closed
    statement_record_id  STRING,    -- FK -> silver_reconciliation_standard (VENDOR_STATEMENT side) -- NULL for Unmatched Record (no statement counterpart at all)
    source_file           STRING,    -- denormalized lineage
    statement_id          STRING,    -- denormalized lineage
    date_raised          TIMESTAMP,
    date_resolved        TIMESTAMP,
    statement_period     STRING
)
USING DELTA
""")

# ---- CELL 7: Gold -- vendor & shop summaries -----------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_vendor_summary (
    vendor_id                 STRING,
    vendor_name               STRING,
    statement_period          STRING,
    total_invoices            INT,
    matched_count             INT,
    exception_count           INT,
    match_pct                 DECIMAL(5,2),
    total_outstanding_amount  DECIMAL(14,2)
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold_shop_summary (
    shop               STRING,
    vendor_id          STRING,
    statement_period   STRING,
    total_invoices     INT,
    matched_count      INT,
    exception_count    INT,
    match_pct          DECIMAL(5,2)
)
USING DELTA
""")

# ---- CELL 8: Gold -- reconciliation summary (management reporting) ------
# This is the table a manager opens: does the vendor's number match our
# number, for this shop, this period, and if not, by how much?
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_reconciliation_summary (
    vendor_id            STRING,
    vendor_name          STRING,
    shop                 STRING,
    statement_period     STRING,
    statement_total      DECIMAL(14,2),  -- sum of outstanding_amount, VENDOR_STATEMENT side
    internal_erp_total   DECIMAL(14,2),  -- sum of outstanding_amount, INTERNAL_ERP side
    difference           DECIMAL(14,2),  -- statement_total - internal_erp_total
    total_invoice_count  INT,
    matched_count        INT,
    exception_count      INT,
    match_pct            DECIMAL(5,2),
    overall_status        STRING          -- RECONCILED | MINOR_VARIANCE | EXCEPTIONS_PRESENT
)
USING DELTA
""")

# ---- CELL 9: Validation -- mutation manifest (ground truth) -------------
# Written by the Mock ERP Generator, one row per statement invoice,
# recording exactly what scenario was assigned and what the matching
# engine SHOULD produce for it. This is what turns Phase 4/5 from "the
# output looks plausible" into "here are the N cases we planted and
# here's whether the engine got every one right." Lives outside the
# Bronze/Silver/Gold medallion on purpose -- it's test metadata about
# the pipeline, not business data flowing through it.
spark.sql("""
CREATE TABLE IF NOT EXISTS validation_mutation_manifest (
    manifest_id                  STRING,     -- surrogate key
    vendor_id                    STRING,
    statement_period             STRING,
    statement_invoice_number     STRING,     -- the original, as it appears on the vendor statement
    generated_erp_invoice_number STRING,     -- what actually landed in the ERP extract (may be mutated, or absent -- see scenario_type)
    scenario_type                STRING,     -- exact_match | invoice_revision | missing_invoice | amount_mismatch | duplicate_invoice | vendor_reference_issue | missing_credit | pending_posting
    expected_match_status        STRING,     -- MATCHED | EXCEPTION
    expected_match_level         INT,        -- 1, 2, or 3 -- NULL when expected_match_status = 'EXCEPTION'
    expected_exception_reason    STRING,     -- NULL when expected_match_status = 'MATCHED'
    mutation_details             STRING,     -- human-readable: exactly what was changed and by how much
    generator_config_version     STRING,     -- ties back to astech_scenarios.json's generator_version
    generation_timestamp         TIMESTAMP
)
USING DELTA
""")

# ---- CELL 10: Validation -- document review queue -------------------
# Catches anything that fails structural validation, is missing
# mandatory fields, falls below the confidence threshold, or (in later
# phases) fails a Silver-stage business rule. Named for the broader
# scope, not just AI extraction failures -- pipeline_stage records
# WHERE in the pipeline the rejection happened. Nothing landing here is
# ever discarded; review_status tracks it through to resolution.
spark.sql("""
CREATE TABLE IF NOT EXISTS validation_document_review_queue (
    review_id                     STRING,     -- surrogate key
    vendor_id                     STRING,
    source_file                   STRING,
    statement_id                  STRING,     -- nullable -- may not be determinable if extraction failed badly
    statement_period              STRING,
    pipeline_stage                STRING,     -- 'AI_EXTRACTION' | 'VALIDATION' | 'SILVER_NORMALIZATION' (future)
    rejection_category            STRING,     -- 'MALFORMED_JSON' | 'MISSING_MANDATORY_FIELD' | 'INVALID_FIELD_TYPE' | 'LOW_CONFIDENCE' | 'DUPLICATE_RECORD' | 'UNSUPPORTED_LAYOUT' | 'CORRUPTED_PDF' | 'BUSINESS_RULE_VIOLATION' | 'AI_CALL_FAILED' (Phase B: the AI provider call itself failed -- transport/HTTP/auth error -- distinct from 'MALFORMED_JSON', where the call succeeded but the response didn't match the expected schema; this is a comment-only convention, not a DB constraint, so no DDL change accompanies it)
    rejection_details             STRING,     -- human-readable specifics
    extraction_confidence         DECIMAL(5,4), -- nullable
    confidence_threshold_applied  DECIMAL(5,4), -- what threshold was configured at the time -- thresholds can change
    raw_payload                   STRING,     -- the raw (possibly malformed) extracted data, verbatim, for manual review
    ai_audit_id                   STRING,     -- FK -> ai_audit_log, so a reviewer can trace back to the exact AI call
    review_status                 STRING,     -- 'PENDING_REVIEW' | 'IN_REVIEW' | 'RESOLVED_REPROCESSED' | 'RESOLVED_DISCARDED' | 'RESOLVED_MANUAL_ENTRY'
    flagged_timestamp             TIMESTAMP,
    reviewed_by                   STRING,     -- nullable until resolved
    reviewed_timestamp            TIMESTAMP,  -- nullable until resolved
    resolution_notes              STRING      -- nullable until resolved
)
USING DELTA
""")

# ---- CELL 11: AI audit / lineage log (observability only) ---------------
# Every AI interaction, any type, gets one row here -- extraction today,
# exception explanation and executive summary in later phases. This
# table is PURELY for operational monitoring and debugging: it never
# participates in reconciliation logic and is never joined into any
# Gold table. Built via src/ai/audit_logger.py so the same shape is
# produced no matter which service or provider generated the call.
spark.sql("""
CREATE TABLE IF NOT EXISTS ai_audit_log (
    audit_id                STRING,     -- surrogate key
    source_file              STRING,
    vendor_id                STRING,
    statement_id             STRING,     -- nullable
    interaction_type         STRING,     -- 'EXTRACTION' | 'EXCEPTION_EXPLANATION' | 'EXECUTIVE_SUMMARY'
    ai_provider               STRING,     -- e.g. 'gemini'
    model                    STRING,
    prompt_version           STRING,     -- versioned identifier, e.g. 'extraction_v1' -- NEVER the full prompt text
    request_timestamp        TIMESTAMP,
    latency_ms               DOUBLE,
    attempt_count            INT,        -- number of HTTP attempts, including retries
    success                  BOOLEAN,
    response_status          STRING,     -- 'SUCCESS' | 'HTTP_ERROR' | 'TRANSPORT_ERROR' | 'PARSE_ERROR' | 'MISSING_API_KEY' | 'UNKNOWN_ERROR'
    error_message            STRING,     -- nullable
    extraction_confidence    DECIMAL(5,4), -- nullable -- only meaningful when interaction_type = 'EXTRACTION'
    validation_result        STRING      -- nullable -- populated once validate_extraction() has run on this call's output
)
USING DELTA
""")

# ---- CELL 12: Confirm what we just built ----------------------------------
print("Lakehouse schema ready. Tables:")
for t in spark.catalog.listTables():
    print(f"  - {t.name}")
