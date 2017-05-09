# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncpg
from asyncpg import _testbase as tb


class TestCacheInvalidation(tb.ConnectedTestCase):
    async def test_prepare_cache_invalidation_silent(self):
        await self.con.execute('CREATE TABLE tab1(a int, b int)')

        try:
            await self.con.execute('INSERT INTO tab1 VALUES (1, 2)')
            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, 2))

            await self.con.execute(
                'ALTER TABLE tab1 ALTER COLUMN b SET DATA TYPE text')

            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, '2'))
        finally:
            await self.con.execute('DROP TABLE tab1')

    async def test_prepare_cache_invalidation_in_transaction(self):
        await self.con.execute('CREATE TABLE tab1(a int, b int)')

        try:
            await self.con.execute('INSERT INTO tab1 VALUES (1, 2)')
            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, 2))

            await self.con.execute(
                'ALTER TABLE tab1 ALTER COLUMN b SET DATA TYPE text')

            with self.assertRaisesRegex(asyncpg.InvalidCachedStatementError,
                                        'cached statement plan is invalid'):
                async with self.con.transaction():
                    result = await self.con.fetchrow('SELECT * FROM tab1')

            # This is now OK,
            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, '2'))
        finally:
            await self.con.execute('DROP TABLE tab1')

    async def test_prepare_cache_invalidation_in_pool(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=2, max_size=2)

        await self.con.execute('CREATE TABLE tab1(a int, b int)')

        try:
            await self.con.execute('INSERT INTO tab1 VALUES (1, 2)')

            con1 = await pool.acquire()
            con2 = await pool.acquire()

            result = await con1.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, 2))

            result = await con2.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, 2))

            await self.con.execute(
                'ALTER TABLE tab1 ALTER COLUMN b SET DATA TYPE text')

            # con1 tries the same plan, will invalidate the cache
            # for the entire pool.
            result = await con1.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, '2'))

            async with con2.transaction():
                # This should work, as con1 should have invalidated
                # the plan cache.
                result = await con2.fetchrow('SELECT * FROM tab1')
                self.assertEqual(result, (1, '2'))

        finally:
            await self.con.execute('DROP TABLE tab1')
            await pool.close()
