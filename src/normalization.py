"""
normalization.py

Purpose
-------
Before the matching engine (Phase 4) compares invoice numbers, revisions
and reissues need to be collapsed back to their original identifier.
Vendors reissue the same invoice with a suffix when they correct or
re-send it -- Fred Beans uses "X1"/"X2", VINART uses trailing "R" for
reversals, some vendors use "-1"/"-2". None of that is asTech-specific
or hardcoded here: every vendor's config supplies its own list of
`revision_suffixes` (regex patterns), and this function just applies
whichever list it's given.

Design notes
------------
- Pure function, no Spark dependency -- this is what makes it trivially
  unit-testable (see tests/test_normalization.py) and reusable as a
  plain Python function OR wrapped as a Spark UDF (see udf() below).
- Suffixes are stripped repeatedly (not just once) because a single
  invoice can carry more than one revision marker over its life
  (e.g. Fred Beans' "9298046X1" style chains). We loop until no more
  patterns match, up to a safety cap.
- Matching is anchored to the END of the string ($ in each pattern)
  by convention in the vendor config, so we never accidentally strip
  a valid substring from the middle of a real invoice number.
"""

import re

MAX_STRIP_ITERATIONS = 5  # safety cap -- stops a bad config from looping forever


def normalize_invoice_number(raw_invoice_number: str, revision_suffixes: list[str]) -> str:
    """
    Strip vendor-configured revision suffixes from an invoice number.

    Parameters
    ----------
    raw_invoice_number : the invoice number as extracted, e.g. "9298046X1"
    revision_suffixes   : list of regex patterns from the vendor config,
                          e.g. ["X\\d+$", "-\\d+$", "R$"]

    Returns
    -------
    The normalized invoice number, e.g. "9298046".
    Returns the original value unchanged if it's None/empty or if no
    pattern matches -- normalization should never fail loudly on a
    line it doesn't recognize, since the line still needs to flow
    through to matching (and, if genuinely unmatched, to Section 8's
    Exception Table) rather than being dropped here.
    """
    if not raw_invoice_number:
        return raw_invoice_number

    normalized = raw_invoice_number.strip()

    for _ in range(MAX_STRIP_ITERATIONS):
        stripped_this_pass = False
        for pattern in revision_suffixes:
            match = re.search(pattern, normalized)
            # Guard: only strip if something real remains before the
            # suffix. Without this, a pattern like "X\d+$" can match an
            # entire invoice number that happens to look like "X" +
            # digits (e.g. "X1200241"), wiping it out completely.
            if match and match.start() > 0:
                normalized = normalized[: match.start()]
                stripped_this_pass = True
        if not stripped_this_pass:
            break

    return normalized


def make_spark_udf():
    """
    Wraps normalize_invoice_number as a Spark UDF, curried with a
    specific vendor's revision_suffixes list, for use inside a Silver
    or matching-engine DataFrame transformation (Phase 3/4).

    Usage in a notebook:
        from src.normalization import make_spark_udf
        normalize_udf = make_spark_udf()
        df = df.withColumn(
            "invoice_number_normalized",
            normalize_udf(F.col("invoice_number"), F.array([F.lit(p) for p in revision_suffixes]))
        )
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    @udf(returnType=StringType())
    def _udf(raw_invoice_number, revision_suffixes):
        return normalize_invoice_number(raw_invoice_number, list(revision_suffixes or []))

    return _udf
