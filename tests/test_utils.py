# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import datetime

from asyncpg import utils
from asyncpg import _testbase as tb


class TestUtils(tb.ConnectedTestCase):

    async def test_mogrify_simple(self):
        cases = [
            ('timestamp',
                datetime.datetime(2016, 10, 10),
                "SELECT '2016-10-10 00:00:00'::timestamp"),
            ('int[]',
                [[1, 2], [3, 4]],
                "SELECT '{{1,2},{3,4}}'::int[]"),
        ]

        for typename, data, expected in cases:
            with self.subTest(value=data, type=typename):
                mogrified = await utils._mogrify(
                    self.con, 'SELECT $1::{}'.format(typename), [data])
                self.assertEqual(mogrified, expected)

    async def test_mogrify_multiple(self):
        mogrified = await utils._mogrify(
            self.con, 'SELECT $1::int, $2::int[]',
            [1, [2, 3, 4, 5]])
        expected = "SELECT '1'::int, '{2,3,4,5}'::int[]"
        self.assertEqual(mogrified, expected)
