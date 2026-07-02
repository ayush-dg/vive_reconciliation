"""
mutations.py

Purpose
-------
One function per reconciliation scenario (config/mock_erp/astech_scenarios.json's
scenario_mix keys). Each takes one Vendor Statement invoice (a plain dict,
as normalized in silver_reconciliation_standard), a shared random.Random
instance, and that scenario's mutation_parameters sub-dict, and returns the
scenario-varying ERP-side fields only: invoice_number, amount,
outstanding_amount, ro_number.

What these do NOT do
----------------------
Common envelope fields every ERP record needs regardless of scenario --
status, posting_date, po_number, vendor/shop passthrough, row_number -- are
deliberately NOT built here. That's generator.py's job, driven centrally by
config/mock_erp/astech_scenarios.json's erp_status_by_scenario and
field_generation, so that logic exists in exactly one place instead of
eight near-identical copies.

Amounts are returned as fixed-precision STRINGS (e.g. "48.75"), not floats
or Decimals -- bronze_internal_erp_raw's raw_amount/raw_outstanding_amount
columns are untyped STRING, same as every other Bronze raw_* column, cast
deliberately later by Silver normalization.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class MutationResult:
    erp_records: list              # 0, 1, or 2 dicts: {invoice_number, amount, outstanding_amount, ro_number}
    generated_erp_invoice_number: Optional[str]  # for the manifest -- what actually "landed" (None for missing_invoice)
    mutation_details: str          # human-readable, written verbatim to validation_mutation_manifest


def _format_amount(value) -> str:
    return f"{float(value):.2f}"


def mutate_exact_match(invoice: dict, rng, params: dict) -> MutationResult:
    amount = _format_amount(invoice["outstanding_amount"])
    record = {
        "invoice_number": invoice["invoice_number"],
        "amount": amount,
        "outstanding_amount": amount,
        "ro_number": invoice["ro_number"],
    }
    return MutationResult([record], invoice["invoice_number"], "ERP record mirrors the statement exactly.")


def mutate_invoice_revision(invoice: dict, rng, params: dict) -> MutationResult:
    suffixes = params.get("suffixes_to_apply", [])
    suffix = rng.choice(suffixes) if suffixes else ""
    revised_number = f"{invoice['invoice_number']}{suffix}"
    amount = _format_amount(invoice["outstanding_amount"])
    record = {
        "invoice_number": revised_number,
        "amount": amount,
        "outstanding_amount": amount,
        "ro_number": invoice["ro_number"],
    }
    details = f"ERP re-issued the invoice number with suffix {suffix!r}: {invoice['invoice_number']} -> {revised_number}"
    return MutationResult([record], revised_number, details)


def mutate_missing_invoice(invoice: dict, rng, params: dict) -> MutationResult:
    return MutationResult([], None, "Invoice exists on the statement but is entirely absent from the ERP extract.")


def mutate_amount_mismatch(invoice: dict, rng, params: dict) -> MutationResult:
    variance_pct = rng.uniform(params.get("variance_min_pct", 0.0), params.get("variance_max_pct", 0.0))
    direction = rng.choice([1, -1])
    original = float(invoice["outstanding_amount"])
    mismatched = original * (1 + direction * variance_pct)
    amount = _format_amount(mismatched)
    record = {
        "invoice_number": invoice["invoice_number"],
        "amount": amount,
        "outstanding_amount": amount,
        "ro_number": invoice["ro_number"],
    }
    sign = "+" if direction > 0 else "-"
    details = (f"ERP amount differs from the statement by {sign}{variance_pct * 100:.1f}%: "
               f"{_format_amount(original)} -> {amount}")
    return MutationResult([record], invoice["invoice_number"], details)


def mutate_duplicate_invoice(invoice: dict, rng, params: dict) -> MutationResult:
    extra_copies = params.get("max_duplicate_copies", 1)
    amount = _format_amount(invoice["outstanding_amount"])
    base_record = {
        "invoice_number": invoice["invoice_number"],
        "amount": amount,
        "outstanding_amount": amount,
        "ro_number": invoice["ro_number"],
    }
    records = [dict(base_record) for _ in range(1 + extra_copies)]
    details = f"Invoice appears {len(records)} times in the ERP extract (1 original + {extra_copies} duplicate(s))."
    return MutationResult(records, invoice["invoice_number"], details)


def mutate_vendor_reference_issue(invoice: dict, rng, params: dict) -> MutationResult:
    work_order_number = invoice.get("work_order_number")
    details_prefix = ""
    if not work_order_number:
        # Rare edge case: this statement invoice has no captured work order
        # number to substitute. Flagged explicitly rather than silently
        # using a number that isn't tied to any real field.
        work_order_number = f"WO-SYNTH-{rng.randint(10000000, 99999999)}"
        details_prefix = "No real work_order_number was captured for this invoice -- used a synthesized placeholder. "

    amount = _format_amount(invoice["outstanding_amount"])
    record = {
        "invoice_number": work_order_number,
        "amount": amount,
        "outstanding_amount": amount,
        "ro_number": None,  # no RO carried on this record -- Level 3 can't rescue it either
    }
    details = (f"{details_prefix}ERP references the work order number instead of the invoice number: "
               f"{invoice['invoice_number']} -> {work_order_number} (no RO number carried).")
    return MutationResult([record], work_order_number, details)


def mutate_missing_credit(invoice: dict, rng, params: dict) -> MutationResult:
    credit_pct = rng.uniform(params.get("credit_min_pct", 0.0), params.get("credit_max_pct", 0.0))
    original = float(invoice["outstanding_amount"])
    reduced = original * (1 - credit_pct)
    record = {
        "invoice_number": invoice["invoice_number"],
        "amount": _format_amount(original),
        "outstanding_amount": _format_amount(reduced),
        "ro_number": invoice["ro_number"],
    }
    details = (f"ERP outstanding amount reduced by a {credit_pct * 100:.1f}% credit not on the statement: "
               f"{_format_amount(original)} -> {_format_amount(reduced)}")
    return MutationResult([record], invoice["invoice_number"], details)


def mutate_pending_posting(invoice: dict, rng, params: dict) -> MutationResult:
    amount = _format_amount(invoice["outstanding_amount"])
    record = {
        "invoice_number": invoice["invoice_number"],
        "amount": amount,
        "outstanding_amount": amount,
        "ro_number": invoice["ro_number"],
    }
    return MutationResult([record], invoice["invoice_number"],
                           "Invoice exists on the ERP side but has not finished posting yet.")


MUTATORS = {
    "exact_match": mutate_exact_match,
    "invoice_revision": mutate_invoice_revision,
    "missing_invoice": mutate_missing_invoice,
    "amount_mismatch": mutate_amount_mismatch,
    "duplicate_invoice": mutate_duplicate_invoice,
    "vendor_reference_issue": mutate_vendor_reference_issue,
    "missing_credit": mutate_missing_credit,
    "pending_posting": mutate_pending_posting,
}
