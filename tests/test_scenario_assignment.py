"""
tests/test_scenario_assignment.py

Validates src/mock_erp/scenario_assignment.py -- proportional, reproducible
assignment of statement invoices to reconciliation scenarios.
"""

import sys
import os
from collections import Counter
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.mock_erp.scenario_assignment import assign_scenarios

SCENARIO_MIX = {
    "exact_match": 0.75,
    "invoice_revision": 0.05,
    "missing_invoice": 0.08,
    "amount_mismatch": 0.05,
    "duplicate_invoice": 0.03,
    "vendor_reference_issue": 0.02,
    "missing_credit": 0.01,
    "pending_posting": 0.01,
}


def _invoices(n):
    return [{"invoice_number": f"SIN{i:05d}"} for i in range(n)]


def test_every_invoice_assigned_exactly_once():
    invoices = _invoices(202)
    assigned = assign_scenarios(invoices, SCENARIO_MIX, seed=42)
    assert len(assigned) == 202
    assigned_invoice_numbers = [inv["invoice_number"] for inv, _ in assigned]
    assert sorted(assigned_invoice_numbers) == sorted(inv["invoice_number"] for inv in invoices)


def test_proportions_within_rounding_of_configured_mix():
    invoices = _invoices(202)
    assigned = assign_scenarios(invoices, SCENARIO_MIX, seed=42)
    counts = Counter(scenario for _, scenario in assigned)
    for scenario, pct in SCENARIO_MIX.items():
        target = round(pct * 202)
        # allow +/-1 for the scenario absorbing the rounding remainder
        assert abs(counts.get(scenario, 0) - target) <= 1, (scenario, counts.get(scenario, 0), target)
    assert sum(counts.values()) == 202


def test_reproducible_with_same_seed():
    invoices = _invoices(202)
    first = assign_scenarios(invoices, SCENARIO_MIX, seed=42)
    second = assign_scenarios(invoices, SCENARIO_MIX, seed=42)
    assert [(inv["invoice_number"], s) for inv, s in first] == [(inv["invoice_number"], s) for inv, s in second]


def test_different_seed_can_produce_different_assignment():
    invoices = _invoices(202)
    first = assign_scenarios(invoices, SCENARIO_MIX, seed=42)
    second = assign_scenarios(invoices, SCENARIO_MIX, seed=7)
    first_map = {inv["invoice_number"]: s for inv, s in first}
    second_map = {inv["invoice_number"]: s for inv, s in second}
    assert first_map != second_map


def test_scenario_counts_sum_to_total_even_with_rounding_drift():
    # 7 invoices: every target rounds to 0 or 1, drift must be absorbed
    # without losing or double-counting an invoice.
    invoices = _invoices(7)
    assigned = assign_scenarios(invoices, SCENARIO_MIX, seed=42)
    assert len(assigned) == 7


def test_empty_invoice_list_returns_empty_assignment():
    assert assign_scenarios([], SCENARIO_MIX, seed=42) == []


def test_does_not_mutate_input_list():
    invoices = _invoices(10)
    original_order = list(invoices)
    assign_scenarios(invoices, SCENARIO_MIX, seed=42)
    assert invoices == original_order


def test_single_scenario_mix_assigns_everyone_to_it():
    invoices = _invoices(5)
    assigned = assign_scenarios(invoices, {"exact_match": 1.0}, seed=1)
    assert all(scenario == "exact_match" for _, scenario in assigned)
    assert len(assigned) == 5


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
