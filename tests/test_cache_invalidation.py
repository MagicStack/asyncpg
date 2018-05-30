# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncpg
from asyncpg import _testbase as tb

ERRNUM = 'unexpected number of attributes of composite type'
ERRTYP = 'unexpected data type of composite type'


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
            await pool.release(con2)
            await pool.release(con1)
            await pool.close()

    async def test_type_cache_invalidation_in_transaction(self):
        await self.con.execute('CREATE TYPE typ1 AS (x int, y int)')
        await self.con.execute('CREATE TABLE tab1(a int, b typ1)')

        try:
            await self.con.execute('INSERT INTO tab1 VALUES (1, (2, 3))')
            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (2, 3)))

            async with self.con.transaction():
                await self.con.execute('ALTER TYPE typ1 ADD ATTRIBUTE c text')
                with self.assertRaisesRegex(
                        asyncpg.OutdatedSchemaCacheError, ERRNUM):
                    await self.con.fetchrow('SELECT * FROM tab1')
                # The second request must be correct (cache was dropped):
                result = await self.con.fetchrow('SELECT * FROM tab1')
                self.assertEqual(result, (1, (2, 3, None)))

            # This is now OK, the cache is actual after the transaction.
            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (2, 3, None)))
        finally:
            await self.con.execute('DROP TABLE tab1')
            await self.con.execute('DROP TYPE typ1')

    async def test_type_cache_invalidation_in_cancelled_transaction(self):
        await self.con.execute('CREATE TYPE typ1 AS (x int, y int)')
        await self.con.execute('CREATE TABLE tab1(a int, b typ1)')

        try:
            await self.con.execute('INSERT INTO tab1 VALUES (1, (2, 3))')
            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (2, 3)))

            try:
                async with self.con.transaction():
                    await self.con.execute(
                        'ALTER TYPE typ1 ADD ATTRIBUTE c text')
                    with self.assertRaisesRegex(
                            asyncpg.OutdatedSchemaCacheError, ERRNUM):
                        await self.con.fetchrow('SELECT * FROM tab1')
                    # The second request must be correct (cache was dropped):
                    result = await self.con.fetchrow('SELECT * FROM tab1')
                    self.assertEqual(result, (1, (2, 3, None)))
                    raise UserWarning  # Just to generate ROLLBACK
            except UserWarning:
                pass

            with self.assertRaisesRegex(
                    asyncpg.OutdatedSchemaCacheError, ERRNUM):
                await self.con.fetchrow('SELECT * FROM tab1')
            # This is now OK, the cache is filled after being dropped.
            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (2, 3)))
        finally:
            await self.con.execute('DROP TABLE tab1')
            await self.con.execute('DROP TYPE typ1')

    async def test_prepared_type_cache_invalidation(self):
        await self.con.execute('CREATE TYPE typ1 AS (x int, y int)')
        await self.con.execute('CREATE TABLE tab1(a int, b typ1)')

        try:
            await self.con.execute('INSERT INTO tab1 VALUES (1, (2, 3))')
            prep = await self.con._prepare('SELECT * FROM tab1',
                                           use_cache=True)
            result = await prep.fetchrow()
            self.assertEqual(result, (1, (2, 3)))

            try:
                async with self.con.transaction():
                    await self.con.execute(
                        'ALTER TYPE typ1 ADD ATTRIBUTE c text')
                    with self.assertRaisesRegex(
                            asyncpg.OutdatedSchemaCacheError, ERRNUM):
                        await prep.fetchrow()
                    # PS has its local cache for types codecs, even after the
                    # cache cleanup it is not possible to use it.
                    # That's why it is marked as closed.
                    with self.assertRaisesRegex(
                            asyncpg.InterfaceError,
                            'the prepared statement is closed'):
                        await prep.fetchrow()

                    prep = await self.con._prepare('SELECT * FROM tab1',
                                                   use_cache=True)
                    # The second PS must be correct (cache was dropped):
                    result = await prep.fetchrow()
                    self.assertEqual(result, (1, (2, 3, None)))
                    raise UserWarning  # Just to generate ROLLBACK
            except UserWarning:
                pass

            with self.assertRaisesRegex(
                    asyncpg.OutdatedSchemaCacheError, ERRNUM):
                await prep.fetchrow()

            # Reprepare it again after dropping cache.
            prep = await self.con._prepare('SELECT * FROM tab1',
                                           use_cache=True)
            # This is now OK, the cache is filled after being dropped.
            result = await prep.fetchrow()
            self.assertEqual(result, (1, (2, 3)))
        finally:
            await self.con.execute('DROP TABLE tab1')
            await self.con.execute('DROP TYPE typ1')

    async def test_type_cache_invalidation_on_drop_type_attr(self):
        await self.con.execute('CREATE TYPE typ1 AS (x int, y int, c text)')
        await self.con.execute('CREATE TABLE tab1(a int, b typ1)')

        try:
            await self.con.execute(
                'INSERT INTO tab1 VALUES (1, (2, 3, $1))', 'x')
            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (2, 3, 'x')))

            await self.con.execute('ALTER TYPE typ1 DROP ATTRIBUTE x')
            with self.assertRaisesRegex(
                    asyncpg.OutdatedSchemaCacheError, ERRNUM):
                await self.con.fetchrow('SELECT * FROM tab1')

            # This is now OK, the cache is filled after being dropped.
            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (3, 'x')))

        finally:
            await self.con.execute('DROP TABLE tab1')
            await self.con.execute('DROP TYPE typ1')

    async def test_type_cache_invalidation_on_change_attr(self):
        await self.con.execute('CREATE TYPE typ1 AS (x int, y int)')
        await self.con.execute('CREATE TABLE tab1(a int, b typ1)')

        try:
            await self.con.execute('INSERT INTO tab1 VALUES (1, (2, 3))')
            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (2, 3)))

            # It is slightly artificial, but can take place in transactional
            # schema changing. Nevertheless, if the code checks and raises it
            # the most probable reason is a difference with the cache type.
            await self.con.execute('ALTER TYPE typ1 DROP ATTRIBUTE y')
            await self.con.execute('ALTER TYPE typ1 ADD ATTRIBUTE y bigint')
            with self.assertRaisesRegex(
                    asyncpg.OutdatedSchemaCacheError, ERRTYP):
                await self.con.fetchrow('SELECT * FROM tab1')

            # This is now OK, the cache is filled after being dropped.
            result = await self.con.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (2, None)))

        finally:
            await self.con.execute('DROP TABLE tab1')
            await self.con.execute('DROP TYPE typ1')

    async def test_type_cache_invalidation_in_pool(self):
        await self.con.execute('CREATE DATABASE testdb')
        pool = await self.create_pool(database='postgres',
                                      min_size=2, max_size=2)

        pool_chk = await self.create_pool(database='testdb',
                                          min_size=2, max_size=2)

        await self.con.execute('CREATE TYPE typ1 AS (x int, y int)')
        await self.con.execute('CREATE TABLE tab1(a int, b typ1)')

        try:
            await self.con.execute('INSERT INTO tab1 VALUES (1, (2, 3))')

            con1 = await pool.acquire()
            con2 = await pool.acquire()

            result = await con1.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (2, 3)))

            result = await con2.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (2, 3)))

            # Create the same schema in the "testdb", fetch data which caches
            # type info.
            con_chk = await pool_chk.acquire()
            await con_chk.execute('CREATE TYPE typ1 AS (x int, y int)')
            await con_chk.execute('CREATE TABLE tab1(a int, b typ1)')
            await con_chk.execute('INSERT INTO tab1 VALUES (1, (2, 3))')
            result = await con_chk.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (2, 3)))

            # Change schema in the databases.
            await self.con.execute('ALTER TYPE typ1 ADD ATTRIBUTE c text')
            await con_chk.execute('ALTER TYPE typ1 ADD ATTRIBUTE c text')

            # con1 tries to get cached type info, fails, but invalidates the
            # cache for the entire pool.
            with self.assertRaisesRegex(
                    asyncpg.OutdatedSchemaCacheError, ERRNUM):
                await con1.fetchrow('SELECT * FROM tab1')

            async with con2.transaction():
                # This should work, as con1 should have invalidated all caches.
                result = await con2.fetchrow('SELECT * FROM tab1')
                self.assertEqual(result, (1, (2, 3, None)))

            # After all the con1 uses actual info from renewed cache entry.
            result = await con1.fetchrow('SELECT * FROM tab1')
            self.assertEqual(result, (1, (2, 3, None)))

            # Check the invalidation is database-specific, i.e. cache entries
            # for pool_chk/con_chk was not dropped via pool/con1.
            with self.assertRaisesRegex(
                    asyncpg.OutdatedSchemaCacheError, ERRNUM):
                await con_chk.fetchrow('SELECT * FROM tab1')

        finally:
            await self.con.execute('DROP TABLE tab1')
            await self.con.execute('DROP TYPE typ1')
            await pool.release(con2)
            await pool.release(con1)
            await pool.close()
            await pool_chk.release(con_chk)
            await pool_chk.close()
            await self.con.execute('DROP DATABASE testdb')
