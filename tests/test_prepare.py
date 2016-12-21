# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import asyncpg
import gc
import unittest

from asyncpg import _testbase as tb


class TestPrepare(tb.ConnectedTestCase):

    async def test_prepare_01(self):
        self.assertEqual(self.con._protocol.queries_count, 0)
        st = await self.con.prepare('SELECT 1 = $1 AS test')
        self.assertEqual(self.con._protocol.queries_count, 0)
        self.assertEqual(st.get_query(), 'SELECT 1 = $1 AS test')

        rec = await st.fetchrow(1)
        self.assertEqual(self.con._protocol.queries_count, 1)
        self.assertTrue(rec['test'])
        self.assertEqual(len(rec), 1)

        self.assertEqual(False, await st.fetchval(10))
        self.assertEqual(self.con._protocol.queries_count, 2)

    async def test_prepare_02(self):
        with self.assertRaisesRegex(Exception, 'column "a" does not exist'):
            await self.con.prepare('SELECT a')

    async def test_prepare_03(self):
        cases = [
            ('text', ("'NULL'", 'NULL'), [
                'aaa',
                None
            ]),

            ('decimal', ('0', 0), [
                123,
                123.5,
                None
            ])
        ]

        for type, (none_name, none_val), vals in cases:
            st = await self.con.prepare('''
                    SELECT CASE WHEN $1::{type} IS NULL THEN {default}
                    ELSE $1::{type} END'''.format(
                type=type, default=none_name))

            for val in vals:
                with self.subTest(type=type, value=val):
                    res = await st.fetchval(val)
                    if val is None:
                        self.assertEqual(res, none_val)
                    else:
                        self.assertEqual(res, val)

    async def test_prepare_04(self):
        s = await self.con.prepare('SELECT $1::smallint')
        self.assertEqual(await s.fetchval(10), 10)

        s = await self.con.prepare('SELECT $1::smallint * 2')
        self.assertEqual(await s.fetchval(10), 20)

        s = await self.con.prepare('SELECT generate_series(5,10)')
        self.assertEqual(await s.fetchval(), 5)
        # Since the "execute" message was sent with a limit=1,
        # we will receive a PortalSuspended message, instead of
        # CommandComplete.  Which means there will be no status
        # message set.
        self.assertIsNone(s.get_statusmsg())
        # Repeat the same test for 'fetchrow()'.
        self.assertEqual(await s.fetchrow(), (5,))
        self.assertIsNone(s.get_statusmsg())

    async def test_prepare_05_unknownoid(self):
        s = await self.con.prepare("SELECT 'test'")
        self.assertEqual(await s.fetchval(), 'test')

    async def test_prepare_06_interrupted_close(self):
        stmt = await self.con.prepare('''SELECT pg_sleep(10)''')
        fut = self.loop.create_task(stmt.fetch())

        await asyncio.sleep(0.2, loop=self.loop)

        self.assertFalse(self.con.is_closed())
        await self.con.close()
        self.assertTrue(self.con.is_closed())

        with self.assertRaisesRegex(asyncpg.ConnectionDoesNotExistError,
                                    'closed in the middle'):
            await fut

        # Test that it's OK to call close again
        await self.con.close()

    async def test_prepare_07_interrupted_terminate(self):
        stmt = await self.con.prepare('''SELECT pg_sleep(10)''')
        fut = self.loop.create_task(stmt.fetchval())

        await asyncio.sleep(0.2, loop=self.loop)

        self.assertFalse(self.con.is_closed())
        self.con.terminate()
        self.assertTrue(self.con.is_closed())

        with self.assertRaisesRegex(asyncpg.ConnectionDoesNotExistError,
                                    'closed in the middle'):
            await fut

        # Test that it's OK to call terminate again
        self.con.terminate()

    async def test_prepare_08_big_result(self):
        stmt = await self.con.prepare('select generate_series(0,10000)')
        result = await stmt.fetch()

        self.assertEqual(len(result), 10001)
        self.assertEqual(
            [r[0] for r in result],
            list(range(10001)))

    async def test_prepare_09_raise_error(self):
        # Stress test ReadBuffer.read_cstr()
        msg = '0' * 1024 * 100
        query = """
        DO language plpgsql $$
        BEGIN
        RAISE EXCEPTION '{}';
        END
        $$;""".format(msg)

        stmt = await self.con.prepare(query)
        with self.assertRaisesRegex(asyncpg.RaiseError, msg):
            with tb.silence_asyncio_long_exec_warning():
                await stmt.fetchval()

    async def test_prepare_10_stmt_lru(self):
        query = 'select {}'
        cache_max = self.con._stmt_cache_max_size
        iter_max = cache_max * 2 + 11

        # First, we have no cached statements.
        self.assertEqual(len(self.con._stmt_cache), 0)

        stmts = []
        for i in range(iter_max):
            s = await self.con.prepare(query.format(i))
            self.assertEqual(await s.fetchval(), i)
            stmts.append(s)

        # At this point our cache should be full.
        self.assertEqual(len(self.con._stmt_cache), cache_max)
        self.assertTrue(
            all(not s.closed for s in self.con._stmt_cache.values()))

        # Since there are references to the statements (`stmts` list),
        # no statements are scheduled to be closed.
        self.assertEqual(len(self.con._stmts_to_close), 0)

        # Removing refs to statements and preparing a new statement
        # will cause connection to cleanup any stale statements.
        stmts.clear()
        gc.collect()

        # Now we have a bunch of statements that have no refs to them
        # scheduled to be closed.
        self.assertEqual(len(self.con._stmts_to_close), iter_max - cache_max)
        self.assertTrue(all(s.closed for s in self.con._stmts_to_close))
        self.assertTrue(
            all(not s.closed for s in self.con._stmt_cache.values()))

        zero = await self.con.prepare(query.format(0))
        # Hence, all stale statements should be closed now.
        self.assertEqual(len(self.con._stmts_to_close), 0)

        # The number of cached statements will stay the same though.
        self.assertEqual(len(self.con._stmt_cache), cache_max)
        self.assertTrue(
            all(not s.closed for s in self.con._stmt_cache.values()))

        # After closing all statements will be closed.
        await self.con.close()
        self.assertEqual(len(self.con._stmts_to_close), 0)
        self.assertEqual(len(self.con._stmt_cache), 0)

        # An attempt to perform an operation on a closed statement
        # will trigger an error.
        with self.assertRaisesRegex(asyncpg.InterfaceError, 'is closed'):
            await zero.fetchval()

    async def test_prepare_11_stmt_gc(self):
        # Test that prepared statements should stay in the cache after
        # they are GCed.

        # First, we have no cached statements.
        self.assertEqual(len(self.con._stmt_cache), 0)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        # The prepared statement that we'll create will be GCed
        # right await.  However, its state should be still in
        # in the statements LRU cache.
        await self.con.prepare('select 1')
        gc.collect()

        self.assertEqual(len(self.con._stmt_cache), 1)
        self.assertEqual(len(self.con._stmts_to_close), 0)

    async def test_prepare_12_stmt_gc(self):
        # Test that prepared statements are closed when there is no space
        # for them in the LRU cache and there are no references to them.

        # First, we have no cached statements.
        self.assertEqual(len(self.con._stmt_cache), 0)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        cache_max = self.con._stmt_cache_max_size

        stmt = await self.con.prepare('select 100000000')
        self.assertEqual(len(self.con._stmt_cache), 1)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        for i in range(cache_max):
            await self.con.prepare('select {}'.format(i))

        self.assertEqual(len(self.con._stmt_cache), cache_max)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        del stmt
        gc.collect()

        self.assertEqual(len(self.con._stmt_cache), cache_max)
        self.assertEqual(len(self.con._stmts_to_close), 1)

    async def test_prepare_13_connect(self):
        v = await self.con.fetchval(
            'SELECT $1::smallint AS foo', 10, column='foo')
        self.assertEqual(v, 10)

        r = await self.con.fetchrow('SELECT $1::smallint * 2 AS test', 10)
        self.assertEqual(r['test'], 20)

        rows = await self.con.fetch('SELECT generate_series(0,$1::int)', 3)
        self.assertEqual([r[0] for r in rows], [0, 1, 2, 3])

    async def test_prepare_14_explain(self):
        # Test simple EXPLAIN.
        stmt = await self.con.prepare('SELECT typname FROM pg_type')
        plan = await stmt.explain()
        self.assertEqual(plan[0]['Plan']['Relation Name'], 'pg_type')

        # Test "EXPLAIN ANALYZE".
        stmt = await self.con.prepare(
            'SELECT typname, typlen FROM pg_type WHERE typlen > $1')
        plan = await stmt.explain(2, analyze=True)
        self.assertEqual(plan[0]['Plan']['Relation Name'], 'pg_type')
        self.assertIn('Actual Total Time', plan[0]['Plan'])

        # Test that 'EXPLAIN ANALYZE' is executed in a transaction
        # that gets rollbacked.
        tr = self.con.transaction()
        await tr.start()
        try:
            await self.con.execute('CREATE TABLE mytab (a int)')
            stmt = await self.con.prepare(
                'INSERT INTO mytab (a) VALUES (1), (2)')
            plan = await stmt.explain(analyze=True)
            self.assertEqual(plan[0]['Plan']['Operation'], 'Insert')

            # Check that no data was inserted
            res = await self.con.fetch('SELECT * FROM mytab')
            self.assertEqual(res, [])
        finally:
            await tr.rollback()

    async def test_prepare_15_stmt_gc_cache_disabled(self):
        # Test that even if the statements cache is off, we're still
        # cleaning up GCed statements.

        self.assertEqual(len(self.con._stmt_cache), 0)
        self.assertEqual(len(self.con._stmts_to_close), 0)
        # Disable cache
        self.con._stmt_cache_max_size = 0

        stmt = await self.con.prepare('select 100000000')
        self.assertEqual(len(self.con._stmt_cache), 0)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        del stmt
        gc.collect()

        # After GC, _stmts_to_close should contain stmt's state
        self.assertEqual(len(self.con._stmt_cache), 0)
        self.assertEqual(len(self.con._stmts_to_close), 1)

        # Next "prepare" call will trigger a cleanup
        stmt = await self.con.prepare('select 1')
        self.assertEqual(len(self.con._stmt_cache), 0)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        del stmt

    async def test_prepare_16_command_result(self):
        async def status(query):
            stmt = await self.con.prepare(query)
            await stmt.fetch()
            return stmt.get_statusmsg()

        try:
            self.assertEqual(
                await status('CREATE TABLE mytab (a int)'),
                'CREATE TABLE')

            self.assertEqual(
                await status('INSERT INTO mytab (a) VALUES (1), (2)'),
                'INSERT 0 2')

            self.assertEqual(
                await status('SELECT a FROM mytab'),
                'SELECT 2')

            self.assertEqual(
                await status('UPDATE mytab SET a = 3 WHERE a = 1'),
                'UPDATE 1')
        finally:
            self.assertEqual(
                await status('DROP TABLE mytab'),
                'DROP TABLE')

    async def test_prepare_17_stmt_closed_lru(self):
        st = await self.con.prepare('SELECT 1')
        st._state.mark_closed()
        with self.assertRaisesRegex(asyncpg.InterfaceError, 'is closed'):
            await st.fetch()

        st = await self.con.prepare('SELECT 1')
        self.assertEqual(await st.fetchval(), 1)

    async def test_prepare_18_empty_result(self):
        # test EmptyQueryResponse protocol message
        st = await self.con.prepare('')
        self.assertEqual(await st.fetch(), [])
        self.assertIsNone(await st.fetchval())
        self.assertIsNone(await st.fetchrow())

        self.assertEqual(await self.con.fetch(''), [])
        self.assertIsNone(await self.con.fetchval(''))
        self.assertIsNone(await self.con.fetchrow(''))

    async def test_prepare_19_concurrent_calls(self):
        st = self.loop.create_task(self.con.fetchval(
            'SELECT ROW(pg_sleep(0.03), 1)'))

        # Wait for some time to make sure the first query is fully
        # prepared (!) and is now awaiting the results (!!).
        await asyncio.sleep(0.01, loop=self.loop)

        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'another operation'):
            await self.con.execute('SELECT 2')

        self.assertEqual(await st, (None, 1))

    async def test_prepare_20_concurrent_calls(self):
        expected = ((None, 1),)

        for methname, val in [('fetch', [expected]),
                              ('fetchval', expected[0]),
                              ('fetchrow', expected)]:

            with self.subTest(meth=methname):

                meth = getattr(self.con, methname)

                vf = self.loop.create_task(
                    meth('SELECT ROW(pg_sleep(0.03), 1)'))

                await asyncio.sleep(0.01, loop=self.loop)

                with self.assertRaisesRegex(asyncpg.InterfaceError,
                                            'another operation'):
                    await meth('SELECT 2')

                self.assertEqual(await vf, val)

    async def test_prepare_21_errors(self):
        stmt = await self.con.prepare('SELECT 10 / $1::int')

        with self.assertRaises(asyncpg.DivisionByZeroError):
            await stmt.fetchval(0)

        self.assertEqual(await stmt.fetchval(5), 2)

    async def test_prepare_22_empty(self):
        # Support for empty target list was added in PostgreSQL 9.4
        if self.server_version < (9, 4):
            raise unittest.SkipTest(
                'PostgreSQL servers < 9.4 do not support empty target list.')

        result = await self.con.fetchrow('SELECT')
        self.assertEqual(result, ())
        self.assertEqual(repr(result), '<Record>')
