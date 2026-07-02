# VIVE Collision — AI-Enabled Vendor Statement Reconciliation PoC

Proof of Concept for automated vendor statement reconciliation, scoped to
one vendor (asTech / Repairify) and one period (May 2026), per the
Solution Architecture Brief. Built on Microsoft Fabric: Lakehouse, Delta
Tables, PySpark, Power BI, with Gemini as an isolated AI service for
document understanding and exception explanation.

**Status:** Phase A and Phase B complete. Phase A: AI Service Layer,
Validation Layer, Review Queue, Audit Logging, Silver normalization for
the vendor statement side. Phase B: `ExtractionService` drives Gemini as
the primary extraction path for `01_bronze_ingestion.py`, with every
AI-extracted record routed through the Validation Layer into either Bronze
or `validation_document_review_queue`, and every AI call logged to
`ai_audit_log`; the original pdfplumber table extraction is retained,
unchanged, as a configurable fallback -- see "AI Service Layer" below.
Both sides of the reconciliation now exist in Silver: the Mock ERP
Generator (`03_mock_erp_generator.py`, no AI involved) simulates a
NetSuite export by mutating Silver's `VENDOR_STATEMENT` rows per
`config/mock_erp/astech_scenarios.json`, and `04_silver_normalization_erp.py`
normalizes that into `silver_reconciliation_standard` tagged
`record_source = 'INTERNAL_ERP'` -- see "Mock ERP Generator" below. Next:
the deterministic Matching Engine and Gold population.

This is a PoC, not a production system, but every design decision is
made as if it will run in one. Code is written to be pasted directly
into Fabric notebooks; a small environment-detection shim at the top of
each notebook lets the same code also run locally for testing.

## Core design principles (apply throughout, not just to AI)

- **Financial decisions stay deterministic.** Spark's rule-based
  matching engine is the only thing that ever decides whether two
  invoices match. AI never does.
- **AI is isolated behind an interface.** Every AI-dependent module
  depends on `AIClient` (an abstract contract), never on `GeminiClient`
  directly. Swapping providers later is a new adapter class, not a
  rewrite.
- **Nothing is silently discarded.** Records that fail validation or
  fall below a confidence threshold go to a review queue, not the trash.
- **Everything is config-driven.** Vendor quirks, ERP source, mock data
  scenarios, AI parameters, and validation rules all live in `config/`,
  never hardcoded in notebooks.
- **Old adapters are retired, not deleted.** The Payment Voucher
  extraction path and the pdfplumber statement parser are both kept
  fully intact and gated behind config flags, even though a newer
  approach is now active. Precedent set once, followed consistently.

## Folder structure

```
vive_reconciliation_poc/
├── README.md
├── config/
│   ├── vendors/
│   │   └── astech.json              # per-vendor parsing/normalization rules, plus Phase B's "extraction" block (active_method, fallback_method, fallback_on_ai_failure, derive_amount_from_outstanding) -- add a vendor by adding a file here
│   ├── erp/
│   │   └── internal_erp.json        # Internal ERP Dataset adapter switch: mock_erp_generator (active) | payment_voucher (dormant) | netsuite (future)
│   ├── mock_erp/
│   │   └── astech_scenarios.json    # scenario mix, seed, mutation parameters for the Mock ERP Generator
│   ├── validation/
│   │   └── extraction_rules.json    # provider-agnostic structural validation + confidence threshold
│   └── ai/
│       └── gemini.json              # model, temperature, retry policy -- no API key, no hardcoded params
├── notebooks/
│   ├── 00_setup_lakehouse_schema.py         # creates every Bronze/Silver/Gold/validation/audit Delta table
│   ├── 01_bronze_ingestion.py               # vendor statement PDF -> bronze_vendor_statement_raw
│   ├── 02_silver_normalization_statement.py # Bronze -> silver_reconciliation_standard (VENDOR_STATEMENT side)
│   ├── 03_mock_erp_generator.py             # Silver VENDOR_STATEMENT -> bronze_internal_erp_raw + validation_mutation_manifest -- no AI, fully deterministic
│   └── 04_silver_normalization_erp.py       # bronze_internal_erp_raw -> silver_reconciliation_standard (INTERNAL_ERP side)
├── src/
│   ├── normalization.py             # invoice-number revision-suffix normalization, config-driven, unit-tested
│   ├── ai/
│   │   ├── base_client.py           # AIClient abstract interface + AIResponse -- the contract everything else depends on
│   │   ├── gemini_client.py         # Gemini implementation of AIClient, injectable transport, config-driven retries
│   │   ├── audit_logger.py          # turns any AIResponse into one ai_audit_log row
│   │   ├── extraction_service.py    # Phase B: builds the extraction prompt from vendor config, calls AIClient, parses the response into raw invoice-line dicts
│   │   ├── extraction_pipeline.py   # Phase B: Spark-free glue -- standardizes raw records, runs them through the Validation Layer, splits Bronze-bound vs. review-queue-bound, dedupes across pages
│   │   ├── explanation_service.py   # interface defined -- implementation lands in the AI Exception Analysis phase
│   │   └── summary_service.py       # interface defined -- implementation lands in the AI Executive Summary phase
│   ├── validation/
│   │   ├── extraction_validator.py  # deterministic gate: structural checks + confidence evaluation + duplicate detection
│   │   └── review_queue.py          # Phase B: turns a rejected record into one validation_document_review_queue row
│   └── mock_erp/                    # no AI, no Spark -- pure Python, fully unit-tested
│       ├── scenario_assignment.py   # proportionally + reproducibly assigns each statement invoice a scenario, seeded
│       ├── mutations.py             # one function per scenario -- the ERP-side fields that vary by scenario
│       └── generator.py             # orchestrates assignment + mutation + manifest construction; the only entry point notebooks call
├── tests/
│   ├── test_normalization.py
│   ├── test_gemini_client.py        # exercised via injected fake transport -- no network access needed
│   ├── test_extraction_validator.py
│   ├── test_audit_logger.py
│   ├── test_extraction_service.py   # Phase B: exercised via a fake AIClient -- no network access needed
│   ├── test_extraction_pipeline.py  # Phase B: no Spark, no PDF -- canned ExtractionOutcome inputs
│   ├── test_review_queue.py         # Phase B
│   ├── test_scenario_assignment.py  # proportions, reproducibility, no Spark
│   ├── test_mutations.py            # one test group per scenario, no Spark
│   └── test_generator.py            # full runs against the REAL astech_scenarios.json, no Spark
├── sample_data/
│   ├── astech_vendor_statement_may2026.pdf
│   └── astech_payment_voucher_may2026.pdf   # retained for the dormant payment_voucher adapter
├── lakehouse/                        # local stand-in for the Fabric Lakehouse's Tables/ folder (empty; populated at runtime)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── sql/                               # ad-hoc validation queries
├── docs/
├── logs/
└── validation/                        # human-facing notes; the review queue itself is a Delta table, not files here
```

## Fabric workspace components (target deployment)

| Component | Name | Purpose |
|---|---|---|
| Workspace | `VIVE-Reconciliation-PoC` | Isolates the PoC from the production VIVE Collision reporting workspace |
| Lakehouse | `VIVE_Reconciliation_LH` | Holds all Bronze/Silver/Gold/validation/audit tables |
| Notebooks | `00_setup_lakehouse_schema`, `01_bronze_ingestion`, `02_silver_normalization_statement`, `03_mock_erp_generator`, `04_silver_normalization_erp`, plus the Matching Engine notebook(s) to come | One notebook per pipeline stage |
| AI service | Gemini (via `src/ai/`) | Document understanding (extraction) and, in later phases, exception explanation + executive summaries |
| Secrets | Fabric-managed secret / environment variable | `GEMINI_API_KEY` — never stored in code or config |
| Power BI report | `VIVE Reconciliation PoC Dashboard` | Reads only Gold tables (+ AI executive summary, once that phase lands) |

## Data architecture

**Bronze** — raw, as-extracted. Every business field stays untyped
(`STRING`) until Silver casts it deliberately. `bronze_vendor_statement_raw`
carries AI extraction metadata (`extraction_confidence`, `extraction_model`,
`document_section`, `bounding_box`, `raw_gemini_response`) as nullable
columns — populated when an AI provider supplies them, `NULL` for the
pdfplumber fallback path. `bronze_internal_erp_raw` is shaped like a real
ERP invoice extract (not the retired Payment Voucher's free-text lines),
so the eventual NetSuite swap changes nothing structural.

**Silver** — one shared, standard schema (`silver_reconciliation_standard`)
for BOTH sides of the reconciliation, distinguished by `record_source`
(`'VENDOR_STATEMENT'` | `'INTERNAL_ERP'` — the *only* field the future
matching engine reads) and `document_type` (`'VENDOR_STATEMENT'` |
`'PAYMENT_VOUCHER'` | `'MOCK_ERP_EXTRACT'` | future `'NETSUITE_INVOICE'` —
audit-only, never read by matching logic). This split is what makes the
NetSuite swap a zero-downstream-change adapter swap.

**Gold** — business-ready outputs (matched invoices, exceptions, vendor
summary, shop summary, reconciliation summary) — not yet populated;
lands with the Matching Engine phase.

**Cross-cutting, non-business tables:**
- `validation_mutation_manifest` — ground truth written by the Mock ERP
  Generator (one row per statement invoice, `expected_match_status` /
  `expected_match_level` / `expected_exception_reason` copied verbatim
  from `config/mock_erp/astech_scenarios.json`), letting the matching
  engine be verified automatically against known-planted scenarios.
- `validation_document_review_queue` — anything that fails structural
  validation, is missing mandatory fields, or falls below the
  confidence threshold. Named for its broader scope (not just AI
  failures) since it can also catch future Silver-stage business-rule
  violations. Nothing lands here permanently — `review_status` tracks
  it through to resolution.
- `ai_audit_log` — one row per AI interaction, any type, for
  operational monitoring only. Never joined into reconciliation logic.

## AI Service Layer

Everything AI-related sits behind `AIClient` (`src/ai/base_client.py`),
an abstract interface with one method: `generate(prompt) -> AIResponse`.
`GeminiClient` is the only file that knows Gemini's specific wire format.
`ExtractionService`, `ExplanationService`, and `SummaryService` depend
only on `AIClient` — never on `GeminiClient` — so adding
`AzureOpenAIClient` or `ClaudeClient` later means writing one new class,
not touching extraction, explanation, or summary logic.

`GeminiClient` takes an injectable transport function, which is what
lets `tests/test_gemini_client.py` exercise retry logic, error
classification, and request-building without any network access —
useful generally, and a practical necessity in this sandbox, which
cannot reach Google's Gemini endpoint. `ExtractionService` is exercised
the same way, via a fake `AIClient` (`tests/test_extraction_service.py`).

**Ingestion flow (Phase B, live in `01_bronze_ingestion.py`):**
```
Vendor Statement PDF (per page)
        │
        ▼
Gemini API (via ExtractionService, prompt built from the vendor
config's source_column_mapping)
        │
        ▼
Structured JSON
        │
        ▼
extraction_pipeline.process_page: standardize (inject vendor/shop,
rename confidence -> extraction_confidence, derive amount if configured)
        │
        ▼
Validation (src/validation/extraction_validator.py, config-driven)
        │
        ▼
Confidence Evaluation (configurable threshold)
        │
   ┌────┴────┐
   │         │
High Conf.   Low Conf. / Validation Failure
   │         │
   ▼         ▼
Bronze   validation_document_review_queue
```
Every AI call along the way is logged to `ai_audit_log` via
`src/ai/audit_logger.py`, regardless of outcome. If a page's AI call
fails outright (not a partial validation failure — see
`extraction_pipeline.process_page`'s docstring), and
`config/vendors/astech.json`'s `extraction.fallback_on_ai_failure` is
true, that page is reactively re-extracted with the original pdfplumber
table parser instead — Bronze either way, no data lost to a transient AI
failure. Setting `extraction.active_method` to `"pdfplumber_tabular"`
reverts the whole document to the exact Phase A behavior (no validation
gate, no review queue, no audit log — that path was never designed to
need one).

## Mock ERP Generator

A real NetSuite export isn't available for this PoC, so
`03_mock_erp_generator.py` simulates one deterministically -- no AI, no
network, seeded and reproducible. All real logic lives in `src/mock_erp/`
(Spark-free, fully unit-tested); the notebook is a thin orchestrator, same
split as `src/ai/`.

```
Silver VENDOR_STATEMENT rows (ordered + collected deterministically)
        │
        ▼
scenario_assignment.assign_scenarios -- proportional, seeded (random_seed
in config/mock_erp/astech_scenarios.json)
        │
        ▼
mutations.MUTATORS[scenario_type] -- one function per scenario, the
ERP-side fields that actually vary
        │
        ▼
generator.generate_mock_erp -- applies common fields (status, posting_date,
po_number, row_number) centrally, builds one manifest row per invoice
        │
   ┌────┴────┐
   ▼         ▼
bronze_internal_erp_raw   validation_mutation_manifest
```

Two determinism details worth knowing if this is ever re-run and expected
to reproduce exactly: `random.Random(seed)` only reproduces the *shuffle*
correctly if the input row order is itself stable, so the notebook
`.orderBy("invoice_number", "ro_number", "record_id")`s Silver before
`.collect()` — Spark's own row order across runs is not guaranteed
otherwise. And `04_silver_normalization_erp.py`'s `record_id` hash
includes `row_number` (Bronze's "generation sequence" column), unlike the
statement side's hash — without it, the `duplicate_invoice` scenario's two
intentionally-identical ERP rows would collide on the same surrogate key.

`vendor_reference_issue` substitutes the real `work_order_number` (carried
through to Silver as part of this phase — it was captured in Bronze all
along but previously dropped before reaching Silver) for the invoice
number, mirroring an actual asTech data-quality case, and clears
`ro_number` so Level 3 matching can't rescue it either.

## Adding a new vendor later (KSI, Fred Beans, VINART, Quirk)

1. Drop a new config file in `config/vendors/<vendor>.json`.
2. No notebook or AI-service code changes needed — extraction is
   AI-driven and vendor-agnostic; normalization reads the new config.

## Running tests

```
python3 tests/test_normalization.py
python3 tests/test_gemini_client.py
python3 tests/test_extraction_validator.py
python3 tests/test_audit_logger.py
python3 tests/test_extraction_service.py
python3 tests/test_extraction_pipeline.py
python3 tests/test_review_queue.py
python3 tests/test_scenario_assignment.py
python3 tests/test_mutations.py
python3 tests/test_generator.py
```
All ten are self-contained (no pytest dependency, no network access,
no Spark session required) and exit non-zero on any failure.

## Running the pipeline locally (for development/testing only)

Each notebook auto-detects whether `spark` already exists (Fabric) or
needs to be created locally. Locally, Delta's JVM jars require Maven
Central, which most sandboxed environments can't reach — swap
`USING DELTA` for `USING PARQUET` in a scratch copy to validate DDL and
logic; the notebooks committed here use `USING DELTA`, correct for
actual Fabric deployment.
