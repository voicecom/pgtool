"""Internal utility functions for pgtool."""

from __future__ import unicode_literals

import math


def pretty_size(value):
    """Convert a number of bytes into a human-readable string.

    Output is 2...5 characters. Values >= 1000 always produce output in form: x.xxxU, xx.xxU, xxxU, xxxxU.
    """
    exp = int(math.log(value, 1024)) if value > 0 else 0
    unit = 'bkMGTPEZY'[exp]
    if exp == 0:
        return '%d%s' % (value, unit)       # value < 1024, result is always without fractions

    unit_value = value / (1024.0 ** exp)    # value in the relevant units
    places = int(math.log(unit_value, 10))  # number of digits before decimal point
    return '%.*f%s' % (2 - places, unit_value, unit)


def fetch_single_row(c, sql, vars=None):
    c.execute(sql, vars)
    assert c.rowcount == 1, "Unexpected %d rows" % c.rowcount
    return c.fetchone()


def fetch_single_val(c, sql, vars=None):
    return fetch_single_row(c, sql, vars)[0]
