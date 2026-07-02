"""
scenario_assignment.py

Purpose
-------
Assigns each Vendor Statement invoice to exactly one reconciliation
scenario (exact_match, invoice_revision, missing_invoice, ...), respecting
the percentages in config/mock_erp/astech_scenarios.json's scenario_mix,
reproducibly for a given seed.

Nothing here is asTech-specific -- scenario_mix is passed in verbatim from
config, so a different vendor's scenario file changes what gets assigned
without any code change here.

Determinism note
-----------------
random.Random(seed) makes the SHUFFLE reproducible, but that's only half of
determinism: the caller must also pass `invoices` in a stable order to
begin with. Spark's .collect() does not guarantee row order across runs,
so the calling notebook (03_mock_erp_generator.py) is responsible for an
explicit, fully-tie-broken .orderBy(...) before collecting rows -- this
module does not (and cannot) fix an unstable input order.
"""

import random


def assign_scenarios(invoices: list, scenario_mix: dict, seed: int) -> list:
    """
    Returns a list of (invoice, scenario_type) pairs, one per input
    invoice, covering every invoice exactly once.

    Target count per scenario = round(pct * N). Rounding rarely makes
    these sum to exactly N, so the scenario with the largest target count
    absorbs the (positive or negative) remainder -- keeping every other
    scenario's count exactly as configured, since the small scenarios are
    the ones a reviewer is most likely to eyeball and expect to match
    astech_scenarios.json precisely.

    Assignment order (which invoices land in which scenario) comes from
    shuffling a COPY of `invoices` with random.Random(seed), then slicing
    it into buckets in scenario_mix's own key order -- the same order
    already present in the config file, so re-ordering scenario_mix in
    config changes assignment reproducibly too.
    """
    n = len(invoices)
    scenario_types = list(scenario_mix.keys())

    target_counts = {s: round(scenario_mix[s] * n) for s in scenario_types}
    drift = n - sum(target_counts.values())
    if scenario_types:
        largest = max(target_counts, key=target_counts.get)
        target_counts[largest] += drift

    rng = random.Random(seed)
    shuffled = list(invoices)
    rng.shuffle(shuffled)

    assigned = []
    cursor = 0
    for scenario_type in scenario_types:
        count = max(0, target_counts[scenario_type])
        for invoice in shuffled[cursor:cursor + count]:
            assigned.append((invoice, scenario_type))
        cursor += count

    return assigned
