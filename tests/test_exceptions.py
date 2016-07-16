# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncpg
from asyncpg import _testbase as tb


class TestExceptions(tb.ConnectedTestCase):

    def test_exceptions_exported(self):
        for err in ('PostgresError', 'SubstringError', 'InterfaceError'):
            self.assertTrue(hasattr(asyncpg, err))
            self.assertIn(err, asyncpg.__all__)

        for err in ('PostgresMessage',):
            self.assertFalse(hasattr(asyncpg, err))
            self.assertNotIn(err, asyncpg.__all__)

        self.assertIsNone(asyncpg.PostgresError.schema_name)

    async def test_exceptions_unpacking(self):
        with self.assertRaises(asyncpg.UndefinedTableError):
            try:
                await self.con.execute('SELECT * FROM _nonexistent_')
            except asyncpg.UndefinedTableError as e:
                self.assertEqual(e.sqlstate, '42P01')
                self.assertEqual(e.position, '15')
                self.assertEqual(e.query, 'SELECT * FROM _nonexistent_')
                self.assertIsNotNone(e.severity)
                raise
