"""
engine.py

Purpose
-------
The deterministic decision core of the Matching Engine. AI never
participates here -- classify_match() is a pure function of its inputs,
same seed data always produces the same decision.

Design commitment
------------------
Mirrors src/normalization.py's established pattern: one pure Python
function, unit-tested directly (tests/test_matching_engine.py), ALSO
wrapped as a Spark UDF (see notebooks/05_matching_engine.py) so there is
exactly one implementation of the matching rules, not a Python version and
a hand-transliterated Spark version that could silently drift apart.

A Spark UDF is a deliberate, disclosed exception to "avoid Python loops" --
it still runs distributed, per-row, parallelized by Spark across
executors. The anti-pattern this project avoids is `.collect()` then a
driver-side `for` loop, not "any per-row Python code." At this PoC's scale
(~200 rows), the reuse win of one tested implementation outweighs the
minor idiom cost of a UDF over an all-column-expression chain.

Inputs
------
classify_match() takes:
- stmt_invoice: dict for ONE Vendor Statement Silver row -- must have
  invoice_number, invoice_number_normalized, outstanding_amount,
  ro_number, work_order_number, record_id.
- erp_candidates: list of dicts, EVERY Internal ERP Silver row sharing
  this statement invoice's (vendor_id, invoice_number_normalized) --
  already grouped by the caller (Spark groupBy + collect_list in the
  notebook; a plain list literal in tests). Each needs record_id,
  invoice_number, outstanding_amount, amount, ro_number, status, credit.
  0 candidates, 1 candidate, or 2+ (duplicate) are all valid inputs.
- work_order_match_invoice_number: set only when erp_candidates is empty
  AND some ERP row's invoice_number equals this statement invoice's
  work_order_number (the notebook computes this via a separate join).
  Used ONLY to enrich deterministic_reason -- never changes the exception
  category, never produces a match (see vendor_reference_issue's
  documented ground truth in config/mock_erp/astech_scenarios.json).

Matching hierarchy
-------------------
Checked in this order, every level additionally gated on the candidate's
status being 'POSTED' (checked before any amount/RO comparison -- a
PENDING candidate that otherwise agrees on everything must NOT match):
  0. >1 candidate            -> EXCEPTION "Duplicate Invoice"
  0. 0 candidates            -> EXCEPTION "Invoice Missing" (+ Level 4 enrichment)
  0. candidate.status != POSTED -> EXCEPTION "Pending Posting"
  1. amount + RO (both present, equal)   -> MATCHED level 1
  2. amount agree, RO missing either side -> MATCHED level 2
  3. amount agree, RO present both, differ -> MATCHED level 3
  -. amount disagree, credit present, stmt matches ERP's original amount -> EXCEPTION "Missing Credit"
  -. amount disagree, otherwise           -> EXCEPTION "Amount Mismatch"
"""

from dataclasses import dataclass
from typing import Optional

DEFAULT_AMOUNT_TOLERANCE = 0.01


@dataclass
class MatchDecision:
    match_status: str                      # 'MATCHED' | 'EXCEPTION'
    match_level: Optional[int] = None      # 1, 2, or 3 -- None for exceptions
    matched_rule: Optional[str] = None     # short code, e.g. 'LEVEL_1_FULL_MATCH' -- None for exceptions
    match_reason: Optional[str] = None     # human-readable -- None for exceptions
    exception_category: Optional[str] = None    # None when matched
    deterministic_reason: Optional[str] = None  # None when matched
    matched_erp_record_id: Optional[str] = None  # the ERP record_id this decision is based on, when exactly one exists


def _amounts_equal(a, b, tolerance: float = DEFAULT_AMOUNT_TOLERANCE) -> bool:
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tolerance


def classify_match(
    stmt_invoice: dict,
    erp_candidates: list,
    work_order_match_invoice_number: Optional[str] = None,
    tolerance: float = DEFAULT_AMOUNT_TOLERANCE,
) -> MatchDecision:
    # ---- Duplicate: checked before any level logic. Even if amount/RO
    # would otherwise agree, 2+ ERP rows sharing one normalized invoice
    # number is always the exception, never a match.
    if len(erp_candidates) > 1:
        amounts = {c.get("outstanding_amount") for c in erp_candidates}
        identical_note = (
            "all copies have the same outstanding_amount"
            if len(amounts) == 1
            else "the copies do NOT all agree on outstanding_amount -- review individually"
        )
        return MatchDecision(
            match_status="EXCEPTION",
            exception_category="Duplicate Invoice",
            deterministic_reason=(
                f"{len(erp_candidates)} ERP records share invoice number "
                f"{stmt_invoice.get('invoice_number_normalized')!r} (expected exactly 1); {identical_note}."
            ),
        )

    # ---- No ERP candidate at all.
    if len(erp_candidates) == 0:
        reason = f"No ERP record found for invoice {stmt_invoice.get('invoice_number')!r}."
        if work_order_match_invoice_number:
            reason += (
                f" An ERP record with invoice_number={work_order_match_invoice_number!r} matches this "
                f"statement invoice's work_order_number ({stmt_invoice.get('work_order_number')!r}) -- "
                f"likely a vendor reference issue (the ERP recorded the work order instead of the invoice "
                f"number), but this is reported as Invoice Missing since Levels 1-3 cannot confirm it."
            )
        return MatchDecision(match_status="EXCEPTION", exception_category="Invoice Missing", deterministic_reason=reason)

    candidate = erp_candidates[0]

    # ---- PENDING blocks a match at any level, regardless of how well
    # everything else agrees -- checked BEFORE amount/RO comparison.
    status = candidate.get("status")
    if status != "POSTED":
        return MatchDecision(
            match_status="EXCEPTION",
            exception_category="Pending Posting",
            deterministic_reason=(
                f"ERP record for invoice {stmt_invoice.get('invoice_number')!r} has status={status!r}, "
                f"not POSTED -- not yet finished posting."
            ),
            matched_erp_record_id=candidate.get("record_id"),
        )

    amounts_agree = _amounts_equal(stmt_invoice.get("outstanding_amount"), candidate.get("outstanding_amount"), tolerance)
    stmt_ro = stmt_invoice.get("ro_number")
    erp_ro = candidate.get("ro_number")

    if amounts_agree:
        if stmt_ro is not None and erp_ro is not None and stmt_ro == erp_ro:
            return MatchDecision(
                match_status="MATCHED", match_level=1, matched_rule="LEVEL_1_FULL_MATCH",
                match_reason="Invoice number (normalized), amount, and RO number all agree.",
                matched_erp_record_id=candidate.get("record_id"),
            )
        if stmt_ro is None or erp_ro is None:
            return MatchDecision(
                match_status="MATCHED", match_level=2, matched_rule="LEVEL_2_AMOUNT_MATCH_RO_MISSING",
                match_reason="Invoice number (normalized) and amount agree; RO number is missing on at least one side.",
                matched_erp_record_id=candidate.get("record_id"),
            )
        # both ROs present, but differ
        return MatchDecision(
            match_status="MATCHED", match_level=3, matched_rule="LEVEL_3_RO_CONFLICT",
            match_reason=(
                f"Invoice number (normalized) and amount agree; RO numbers differ "
                f"({stmt_ro!r} vs {erp_ro!r}) -- treated as a lower-confidence match, not a failure."
            ),
            matched_erp_record_id=candidate.get("record_id"),
        )

    # ---- Amounts disagree: three-way split so "credit present but doesn't
    # fully explain the gap" is never silently conflated with either a
    # clean Missing Credit or a plain Amount Mismatch.
    credit = candidate.get("credit")
    if credit is not None:
        if _amounts_equal(stmt_invoice.get("outstanding_amount"), candidate.get("amount"), tolerance):
            return MatchDecision(
                match_status="EXCEPTION",
                exception_category="Missing Credit",
                deterministic_reason=(
                    f"ERP's original amount ({candidate.get('amount')}) matches the statement "
                    f"({stmt_invoice.get('outstanding_amount')}), but a credit of {credit} was applied on the "
                    f"ERP side that isn't on the statement, reducing outstanding_amount to "
                    f"{candidate.get('outstanding_amount')}."
                ),
                matched_erp_record_id=candidate.get("record_id"),
            )
        return MatchDecision(
            match_status="EXCEPTION",
            exception_category="Amount Mismatch",
            deterministic_reason=(
                f"Statement outstanding_amount ({stmt_invoice.get('outstanding_amount')}) does not match ERP "
                f"outstanding_amount ({candidate.get('outstanding_amount')}); a credit of {credit} is also present "
                f"on the ERP side but does not fully explain the discrepancy."
            ),
            matched_erp_record_id=candidate.get("record_id"),
        )

    return MatchDecision(
        match_status="EXCEPTION",
        exception_category="Amount Mismatch",
        deterministic_reason=(
            f"Statement outstanding_amount ({stmt_invoice.get('outstanding_amount')}) does not match ERP "
            f"outstanding_amount ({candidate.get('outstanding_amount')})."
        ),
        matched_erp_record_id=candidate.get("record_id"),
    )
