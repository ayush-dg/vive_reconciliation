"""
generator.py

Purpose
-------
Orchestrates the Mock ERP Generator end to end: assign each statement
invoice a scenario (scenario_assignment.py), mutate it (mutations.py), then
apply the fields common to every generated ERP record -- status,
posting_date, po_number, row_number (generation sequence), vendor/shop
passthrough -- in exactly one place, driven entirely by
config/mock_erp/astech_scenarios.json.

Pure Python, no Spark -- the calling notebook (03_mock_erp_generator.py)
converts GeneratorResult.bronze_records into Spark Rows and writes them;
this module never touches a DataFrame.
"""

import random
from dataclasses import dataclass, field
from datetime import timedelta

from .mutations import MUTATORS
from .scenario_assignment import assign_scenarios


@dataclass
class GeneratorResult:
    bronze_records: list = field(default_factory=list)     # ready for Bronze Row-mapping (still plain dicts)
    manifest_records: list = field(default_factory=list)   # one per statement invoice
    expected_bronze_row_count: int = 0                      # == len(bronze_records) -- exposed so the notebook
                                                              # never has to re-derive it via scenario-type arithmetic


def generate_mock_erp(statement_invoices: list, scenarios_config: dict, seed: int) -> GeneratorResult:
    """
    statement_invoices : list of dicts, one per Silver VENDOR_STATEMENT row,
                          each with at least: vendor_id, vendor_name, shop,
                          invoice_number, invoice_date, ro_number,
                          work_order_number, outstanding_amount,
                          statement_id, statement_period. Caller is
                          responsible for passing these in a stable,
                          deterministic order (see scenario_assignment.py's
                          determinism note).
    scenarios_config    : the parsed contents of
                          config/mock_erp/astech_scenarios.json, used as
                          the single source of truth for scenario mix,
                          mutation parameters, ERP status per scenario,
                          expected match outcomes, and field-generation
                          parameters (posting-date lag, PO number prefix).
    seed                : scenarios_config["random_seed"], passed explicitly
                          rather than read from the dict here so a caller
                          can override it (e.g. in tests) without editing
                          config.
    """
    assigned = assign_scenarios(statement_invoices, scenarios_config["scenario_mix"], seed)
    rng = random.Random(seed)  # independent stream from assign_scenarios' own -- same seed, deterministic either way

    erp_status_by_scenario = scenarios_config["erp_status_by_scenario"]
    expected_outcome_by_scenario = scenarios_config["expected_outcome_by_scenario"]
    mutation_parameters = scenarios_config["mutation_parameters"]
    field_generation = scenarios_config["field_generation"]
    po_prefix = field_generation["po_number_prefix"]
    lag_min = field_generation["posting_date_lag_days_min"]
    lag_max = field_generation["posting_date_lag_days_max"]

    bronze_records = []
    manifest_records = []
    generation_sequence = 0

    for invoice, scenario_type in assigned:
        params = mutation_parameters.get(scenario_type, {})
        mutation_result = MUTATORS[scenario_type](invoice, rng, params)
        status = erp_status_by_scenario.get(scenario_type, "POSTED")

        for partial_record in mutation_result.erp_records:
            generation_sequence += 1

            posting_date = None
            if status == "POSTED":
                lag_days = rng.randint(lag_min, lag_max)
                posting_date = invoice["invoice_date"] + timedelta(days=lag_days)

            bronze_records.append({
                "vendor_id": invoice["vendor_id"],
                "vendor": invoice["vendor_name"],
                "shop": invoice["shop"],
                "invoice_number": partial_record["invoice_number"],
                "invoice_date": invoice["invoice_date"],
                "posting_date": posting_date,
                "amount": partial_record["amount"],
                "outstanding_amount": partial_record["outstanding_amount"],
                "ro_number": partial_record["ro_number"],
                "po_number": f"{po_prefix}{generation_sequence:04d}",
                "status": status,
                "statement_id": invoice["statement_id"],
                "statement_period": invoice["statement_period"],
                "row_number": generation_sequence,
            })

        outcome = expected_outcome_by_scenario[scenario_type]
        manifest_records.append({
            "vendor_id": invoice["vendor_id"],
            "statement_period": invoice["statement_period"],
            "statement_invoice_number": invoice["invoice_number"],
            "generated_erp_invoice_number": mutation_result.generated_erp_invoice_number,
            "scenario_type": scenario_type,
            "expected_match_status": outcome["match_status"],
            "expected_match_level": outcome["match_level"],
            "expected_exception_reason": outcome["exception_reason"],
            "mutation_details": mutation_result.mutation_details,
            "generator_config_version": scenarios_config["generator_version"],
        })

    return GeneratorResult(
        bronze_records=bronze_records,
        manifest_records=manifest_records,
        expected_bronze_row_count=len(bronze_records),
    )
