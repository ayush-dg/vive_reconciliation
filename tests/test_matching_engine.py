"""
tests/test_matching_engine.py

Validates src/matching/engine.py's classify_match() -- the single,
deterministic decision function used both here and (via a Spark UDF) by
notebooks/05_matching_engine.py. Every matching level, every exception
category from config/mock_erp/astech_scenarios.json's scenario_mix, and
edge cases not currently exercised by that scenario mix (Levels 2 and 3)
are covered directly with synthetic fixtures -- no Spark, no PDF.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.matching.engine import classify_match, MatchDecision, _amounts_equal

STMT = {
    "invoice_number": "SIN12200241",
    "invoice_number_normalized": "SIN12200241",
    "outstanding_amount": 100.00,
    "ro_number": "RO-1",
    "work_order_number": "24419074",
    "record_id": "stmt-1",
}


def _erp(**overrides):
    base = {
        "record_id": "erp-1",
        "invoice_number": "SIN12200241",
        "outstanding_amount": 100.00,
        "amount": 100.00,
        "ro_number": "RO-1",
        "status": "POSTED",
        "credit": None,
    }
    base.update(overrides)
    return base


def _assert_clean_match(decision, level):
    assert decision.match_status == "MATCHED"
    assert decision.match_level == level
    assert decision.matched_rule is not None
    assert decision.match_reason is not None
    assert decision.exception_category is None
    assert decision.deterministic_reason is None


def _assert_clean_exception(decision, category):
    assert decision.match_status == "EXCEPTION"
    assert decision.exception_category == category
    assert decision.deterministic_reason is not None
    assert decision.match_level is None
    assert decision.matched_rule is None
    assert decision.match_reason is None


# ---- exact_match / invoice_revision -> Level 1 -----------------------------

def test_exact_match_scenario_is_level_1():
    decision = classify_match(STMT, [_erp()])
    _assert_clean_match(decision, 1)
    assert decision.matched_rule == "LEVEL_1_FULL_MATCH"
    assert decision.matched_erp_record_id == "erp-1"


def test_invoice_revision_scenario_still_resolves_at_level_1():
    # The ERP candidate's raw invoice_number carries a revision suffix, but
    # grouping-by-normalized-number already happened upstream (Spark side)
    # before classify_match ever sees it -- amount/RO are unchanged per
    # mutate_invoice_revision, so this must match at Level 1, not some
    # separate "revision" level.
    decision = classify_match(STMT, [_erp(invoice_number="SIN12200241-1")])
    _assert_clean_match(decision, 1)


# ---- missing_invoice / vendor_reference_issue -> Invoice Missing -----------

def test_missing_invoice_scenario_is_invoice_missing_no_enrichment():
    decision = classify_match(STMT, [])
    _assert_clean_exception(decision, "Invoice Missing")
    assert "work order" not in decision.deterministic_reason.lower()


def test_vendor_reference_issue_scenario_stays_invoice_missing_with_enrichment():
    decision = classify_match(STMT, [], work_order_match_invoice_number="24419074")
    _assert_clean_exception(decision, "Invoice Missing")
    assert "24419074" in decision.deterministic_reason
    assert "work order" in decision.deterministic_reason.lower()
    # Explicitly must NOT be promoted to a match or renamed, per the
    # confirmed ground truth.
    assert decision.match_status == "EXCEPTION"


# ---- amount_mismatch -------------------------------------------------------

def test_amount_mismatch_scenario_no_credit_involved():
    decision = classify_match(STMT, [_erp(outstanding_amount=115.00, amount=115.00, credit=None)])
    _assert_clean_exception(decision, "Amount Mismatch")
    assert "credit" not in decision.deterministic_reason.lower()


# ---- duplicate_invoice ------------------------------------------------------

def test_duplicate_invoice_checked_before_any_level_logic():
    # Both copies would otherwise satisfy Level 1 perfectly -- duplicate
    # detection must still win.
    decision = classify_match(STMT, [_erp(record_id="erp-1"), _erp(record_id="erp-2")])
    _assert_clean_exception(decision, "Duplicate Invoice")
    assert "all copies have the same outstanding_amount" in decision.deterministic_reason
    assert decision.matched_erp_record_id is None


def test_duplicate_invoice_with_disagreeing_copies_flags_that_explicitly():
    decision = classify_match(STMT, [_erp(record_id="erp-1", outstanding_amount=100.00),
                                      _erp(record_id="erp-2", outstanding_amount=90.00)])
    _assert_clean_exception(decision, "Duplicate Invoice")
    assert "do NOT all agree" in decision.deterministic_reason


def test_duplicate_invoice_with_three_copies():
    decision = classify_match(STMT, [_erp(record_id=f"erp-{i}") for i in range(3)])
    _assert_clean_exception(decision, "Duplicate Invoice")
    assert "3 ERP records" in decision.deterministic_reason


# ---- missing_credit vs. amount_mismatch discriminator ----------------------

def test_missing_credit_scenario_clean_case():
    # ERP's original amount matches the statement; only outstanding_amount
    # was reduced by a credit.
    decision = classify_match(STMT, [_erp(amount=100.00, outstanding_amount=85.00, credit=15.00)])
    _assert_clean_exception(decision, "Missing Credit")
    assert "15.0" in decision.deterministic_reason


def test_amount_mismatch_with_credit_present_but_not_fully_explaining_gap():
    # credit is present, but the ERP's ORIGINAL amount doesn't match the
    # statement either -- must NOT be misclassified as a clean Missing
    # Credit (this is the three-way branch the design review added).
    decision = classify_match(STMT, [_erp(amount=110.00, outstanding_amount=95.00, credit=15.00)])
    _assert_clean_exception(decision, "Amount Mismatch")
    assert "credit" in decision.deterministic_reason.lower()
    assert "does not fully explain" in decision.deterministic_reason


# ---- pending_posting --------------------------------------------------------

def test_pending_posting_blocks_a_match_even_when_everything_else_agrees():
    decision = classify_match(STMT, [_erp(status="PENDING")])
    _assert_clean_exception(decision, "Pending Posting")
    assert decision.matched_erp_record_id == "erp-1"


def test_non_posted_non_pending_status_is_still_treated_as_pending_posting():
    # Only POSTED/PENDING appear in the current scenario vocabulary;
    # documenting the (disclosed) simplification that any non-POSTED
    # status routes here rather than inventing a new category.
    decision = classify_match(STMT, [_erp(status=None)])
    _assert_clean_exception(decision, "Pending Posting")


# ---- Level 2 / Level 3 (synthetic -- no current mock scenario reaches these) -

def test_level_2_when_ro_missing_on_erp_side():
    decision = classify_match(STMT, [_erp(ro_number=None)])
    _assert_clean_match(decision, 2)
    assert decision.matched_rule == "LEVEL_2_AMOUNT_MATCH_RO_MISSING"


def test_level_2_when_ro_missing_on_statement_side():
    stmt_no_ro = dict(STMT, ro_number=None)
    decision = classify_match(stmt_no_ro, [_erp()])
    _assert_clean_match(decision, 2)


def test_level_2_when_ro_missing_on_both_sides():
    stmt_no_ro = dict(STMT, ro_number=None)
    decision = classify_match(stmt_no_ro, [_erp(ro_number=None)])
    _assert_clean_match(decision, 2)  # absence on both sides is still "missing," not "equal"


def test_level_3_when_ro_present_on_both_sides_but_differs():
    decision = classify_match(STMT, [_erp(ro_number="RO-2")])
    _assert_clean_match(decision, 3)
    assert decision.matched_rule == "LEVEL_3_RO_CONFLICT"
    assert "RO-1" in decision.match_reason and "RO-2" in decision.match_reason


# ---- amount tolerance -------------------------------------------------------

def test_amounts_equal_within_tolerance():
    assert _amounts_equal(100.00, 100.005, tolerance=0.01) is True
    assert _amounts_equal(100.00, 100.02, tolerance=0.01) is False


def test_small_rounding_difference_still_matches_at_level_1():
    decision = classify_match(STMT, [_erp(outstanding_amount=100.005)])
    _assert_clean_match(decision, 1)


def test_amounts_equal_returns_false_when_either_side_is_none():
    assert _amounts_equal(None, 100.00) is False
    assert _amounts_equal(100.00, None) is False


# ---- determinism -------------------------------------------------------------

def test_classify_match_is_deterministic():
    first = classify_match(STMT, [_erp()])
    second = classify_match(STMT, [_erp()])
    assert first == second


def test_classify_match_is_deterministic_across_all_scenario_shapes():
    cases = [
        (STMT, []),
        (STMT, [_erp()]),
        (STMT, [_erp(status="PENDING")]),
        (STMT, [_erp(), _erp()]),
        (STMT, [_erp(outstanding_amount=200.00)]),
    ]
    for stmt, candidates in cases:
        assert classify_match(stmt, candidates) == classify_match(stmt, candidates)


# ---- MatchDecision field cleanliness ----------------------------------------

def test_matched_decisions_never_carry_exception_fields():
    decision = classify_match(STMT, [_erp()])
    assert decision.exception_category is None
    assert decision.deterministic_reason is None


def test_exception_decisions_never_carry_match_fields():
    decision = classify_match(STMT, [])
    assert decision.match_level is None
    assert decision.matched_rule is None
    assert decision.match_reason is None


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
