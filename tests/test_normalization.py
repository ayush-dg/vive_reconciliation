"""
tests/test_normalization.py

Validates src/normalization.py against the exact examples from the
architecture review, plus edge cases worth locking down now so a
future vendor's suffix list can't silently break another vendor's.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.normalization import normalize_invoice_number

# Suffix patterns for the vendors reviewed so far, taken from their configs
ASTECH_SUFFIXES = ["X\\d+$", "-\\d+$", "R$"]
FRED_BEANS_SUFFIXES = ["X\\d+$", "-\\d+$"]
VINART_SUFFIXES = ["R$", "-\\d+$"]


def test_fred_beans_revision_suffix():
    assert normalize_invoice_number("9298046X1", FRED_BEANS_SUFFIXES) == "9298046"
    assert normalize_invoice_number("9298046X2", FRED_BEANS_SUFFIXES) == "9298046"
    assert normalize_invoice_number("9298046X3", FRED_BEANS_SUFFIXES) == "9298046"


def test_quirk_style_dash_suffix():
    assert normalize_invoice_number("46661-1", FRED_BEANS_SUFFIXES) == "46661"


def test_reversal_suffix():
    assert normalize_invoice_number("46661R", VINART_SUFFIXES) == "46661"


def test_chained_suffixes_strip_repeatedly():
    # e.g. a reissued invoice that was ALSO reversed: strip both markers
    assert normalize_invoice_number("46661-1R", VINART_SUFFIXES) == "46661"


def test_no_suffix_present_returns_unchanged():
    assert normalize_invoice_number("SIN12200241", ASTECH_SUFFIXES) == "SIN12200241"


def test_empty_and_none_pass_through_safely():
    assert normalize_invoice_number("", ASTECH_SUFFIXES) == ""
    assert normalize_invoice_number(None, ASTECH_SUFFIXES) is None


def test_suffix_in_middle_of_string_is_not_stripped():
    # Guards against overly loose patterns matching mid-string instead
    # of only at the end -- "X1" here is not a trailing revision marker
    assert normalize_invoice_number("X1200241", ASTECH_SUFFIXES) == "X1200241"


def test_does_not_over_strip_a_real_short_invoice_number():
    # A vendor whose real invoice numbers are short digits shouldn't
    # get accidentally eaten by a dash-suffix rule with no digits left
    assert normalize_invoice_number("100", FRED_BEANS_SUFFIXES) == "100"


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
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
