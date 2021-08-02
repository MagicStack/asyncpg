# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

from itertools import product

from asyncpg.types import Range
from asyncpg import _testbase as tb


class TestTypes(tb.TestCase):

    def test_range_issubset(self):
        subs = [
            Range(empty=True),
            Range(lower=1, upper=5, lower_inc=True, upper_inc=False),
            Range(lower=1, upper=5, lower_inc=True, upper_inc=True),
            Range(lower=1, upper=5, lower_inc=False, upper_inc=True),
            Range(lower=1, upper=5, lower_inc=False, upper_inc=False),
            Range(lower=-5, upper=10),
            Range(lower=2, upper=3),
            Range(lower=1, upper=None),
            Range(lower=None, upper=None)
        ]

        sups = [
            Range(empty=True),
            Range(lower=1, upper=5, lower_inc=True, upper_inc=False),
            Range(lower=1, upper=5, lower_inc=True, upper_inc=True),
            Range(lower=1, upper=5, lower_inc=False, upper_inc=True),
            Range(lower=1, upper=5, lower_inc=False, upper_inc=False),
            Range(lower=None, upper=None)
        ]

        # Each row is 1 subs with all sups
        results = [
            True, True, True, True, True, True,
            False, True, True, False, False, True,
            False, False, True, False, False, True,
            False, False, True, True, False, True,
            False, True, True, True, True, True,
            False, False, False, False, False, True,
            False, True, True, True, True, True,
            False, False, False, False, False, True,
            False, False, False, False, False, True
        ]

        for (sub, sup), res in zip(product(subs, sups), results):
            self.assertIs(
                sub.issubset(sup), res, "Sub:{}, Sup:{}".format(sub, sup)
            )
            self.assertIs(
                sup.issuperset(sub), res, "Sub:{}, Sup:{}".format(sub, sup)
            )
