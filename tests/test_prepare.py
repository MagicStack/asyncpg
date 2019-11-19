# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import asyncpg
import gc
import unittest

from asyncpg import _testbase as tb
from asyncpg import exceptions


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

        await asyncio.sleep(0.2)

        self.assertFalse(self.con.is_closed())
        await self.con.close()
        self.assertTrue(self.con.is_closed())

        with self.assertRaises(asyncpg.QueryCanceledError):
            await fut

        # Test that it's OK to call close again
        await self.con.close()

    async def test_prepare_07_interrupted_terminate(self):
        stmt = await self.con.prepare('''SELECT pg_sleep(10)''')
        fut = self.loop.create_task(stmt.fetchval())

        await asyncio.sleep(0.2)

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
        cache = self.con._stmt_cache

        query = 'select {}'
        cache_max = cache.get_max_size()
        iter_max = cache_max * 2 + 11

        # First, we have no cached statements.
        self.assertEqual(len(cache), 0)

        stmts = []
        for i in range(iter_max):
            s = await self.con._prepare(query.format(i), use_cache=True)
            self.assertEqual(await s.fetchval(), i)
            stmts.append(s)

        # At this point our cache should be full.
        self.assertEqual(len(cache), cache_max)
        self.assertTrue(all(not s.closed for s in cache.iter_statements()))

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
        self.assertTrue(all(not s.closed for s in cache.iter_statements()))

        zero = await self.con.prepare(query.format(0))
        # Hence, all stale statements should be closed now.
        self.assertEqual(len(self.con._stmts_to_close), 0)

        # The number of cached statements will stay the same though.
        self.assertEqual(len(cache), cache_max)
        self.assertTrue(all(not s.closed for s in cache.iter_statements()))

        # After closing all statements will be closed.
        await self.con.close()
        self.assertEqual(len(self.con._stmts_to_close), 0)
        self.assertEqual(len(cache), 0)

        # An attempt to perform an operation on a closed statement
        # will trigger an error.
        with self.assertRaisesRegex(asyncpg.InterfaceError, 'is closed'):
            await zero.fetchval()

    async def test_prepare_11_stmt_gc(self):
        # Test that prepared statements should stay in the cache after
        # they are GCed.

        cache = self.con._stmt_cache

        # First, we have no cached statements.
        self.assertEqual(len(cache), 0)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        # The prepared statement that we'll create will be GCed
        # right await.  However, its state should be still in
        # in the statements LRU cache.
        await self.con._prepare('select 1', use_cache=True)
        gc.collect()

        self.assertEqual(len(cache), 1)
        self.assertEqual(len(self.con._stmts_to_close), 0)

    async def test_prepare_12_stmt_gc(self):
        # Test that prepared statements are closed when there is no space
        # for them in the LRU cache and there are no references to them.

        cache = self.con._stmt_cache
        cache_max = cache.get_max_size()

        # First, we have no cached statements.
        self.assertEqual(len(cache), 0)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        stmt = await self.con._prepare('select 100000000', use_cache=True)
        self.assertEqual(len(cache), 1)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        for i in range(cache_max):
            await self.con._prepare('select {}'.format(i), use_cache=True)

        self.assertEqual(len(cache), cache_max)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        del stmt
        gc.collect()

        self.assertEqual(len(cache), cache_max)
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

        cache = self.con._stmt_cache

        self.assertEqual(len(cache), 0)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        # Disable cache
        cache.set_max_size(0)

        stmt = await self.con._prepare('select 100000000', use_cache=True)
        self.assertEqual(len(cache), 0)
        self.assertEqual(len(self.con._stmts_to_close), 0)

        del stmt
        gc.collect()

        # After GC, _stmts_to_close should contain stmt's state
        self.assertEqual(len(cache), 0)
        self.assertEqual(len(self.con._stmts_to_close), 1)

        # Next "prepare" call will trigger a cleanup
        stmt = await self.con._prepare('select 1', use_cache=True)
        self.assertEqual(len(cache), 0)
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
            'SELECT ROW(pg_sleep(0.1), 1)'))

        # Wait for some time to make sure the first query is fully
        # prepared (!) and is now awaiting the results (!!).
        await asyncio.sleep(0.01)

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
                    meth('SELECT ROW(pg_sleep(0.1), 1)'))

                await asyncio.sleep(0.01)

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

    async def test_prepare_statement_invalid(self):
        await self.con.execute('CREATE TABLE tab1(a int, b int)')

        try:
            await self.con.execute('INSERT INTO tab1 VALUES (1, 2)')

            stmt = await self.con.prepare('SELECT * FROM tab1')

            await self.con.execute(
                'ALTER TABLE tab1 ALTER COLUMN b SET DATA TYPE text')

            with self.assertRaisesRegex(asyncpg.InvalidCachedStatementError,
                                        'cached statement plan is invalid'):
                await stmt.fetchrow()

        finally:
            await self.con.execute('DROP TABLE tab1')

    @tb.with_connection_options(statement_cache_size=0)
    async def test_prepare_23_no_stmt_cache_seq(self):
        self.assertEqual(self.con._stmt_cache.get_max_size(), 0)

        async def check_simple():
            # Run a simple query a few times.
            self.assertEqual(await self.con.fetchval('SELECT 1'), 1)
            self.assertEqual(await self.con.fetchval('SELECT 2'), 2)
            self.assertEqual(await self.con.fetchval('SELECT 1'), 1)

        await check_simple()

        # Run a query that timeouts.
        with self.assertRaises(asyncio.TimeoutError):
            await self.con.fetchrow('select pg_sleep(10)', timeout=0.02)

        # Check that we can run new queries after a timeout.
        await check_simple()

        # Try a cursor/timeout combination. Cursors should always use
        # named prepared statements.
        async with self.con.transaction():
            with self.assertRaises(asyncio.TimeoutError):
                async for _ in self.con.cursor(   # NOQA
                        'select pg_sleep(10)', timeout=0.1):
                    pass

        # Check that we can run queries after a failed cursor
        # operation.
        await check_simple()

    @tb.with_connection_options(max_cached_statement_lifetime=142)
    async def test_prepare_24_max_lifetime(self):
        cache = self.con._stmt_cache

        self.assertEqual(cache.get_max_lifetime(), 142)
        cache.set_max_lifetime(1)

        s = await self.con._prepare('SELECT 1', use_cache=True)
        state = s._state

        s = await self.con._prepare('SELECT 1', use_cache=True)
        self.assertIs(s._state, state)

        s = await self.con._prepare('SELECT 1', use_cache=True)
        self.assertIs(s._state, state)

        await asyncio.sleep(1)

        s = await self.con._prepare('SELECT 1', use_cache=True)
        self.assertIsNot(s._state, state)

    @tb.with_connection_options(max_cached_statement_lifetime=0.5)
    async def test_prepare_25_max_lifetime_reset(self):
        cache = self.con._stmt_cache

        s = await self.con._prepare('SELECT 1', use_cache=True)
        state = s._state

        # Disable max_lifetime
        cache.set_max_lifetime(0)

        await asyncio.sleep(1)

        # The statement should still be cached (as we disabled the timeout).
        s = await self.con._prepare('SELECT 1', use_cache=True)
        self.assertIs(s._state, state)

    @tb.with_connection_options(max_cached_statement_lifetime=0.5)
    async def test_prepare_26_max_lifetime_max_size(self):
        cache = self.con._stmt_cache

        s = await self.con._prepare('SELECT 1', use_cache=True)
        state = s._state

        # Disable max_lifetime
        cache.set_max_size(0)

        s = await self.con._prepare('SELECT 1', use_cache=True)
        self.assertIsNot(s._state, state)

        # Check that nothing crashes after the initial timeout
        await asyncio.sleep(1)

    @tb.with_connection_options(max_cacheable_statement_size=50)
    async def test_prepare_27_max_cacheable_statement_size(self):
        cache = self.con._stmt_cache

        await self.con._prepare('SELECT 1', use_cache=True)
        self.assertEqual(len(cache), 1)

        # Test that long and explicitly created prepared statements
        # are not cached.
        await self.con._prepare("SELECT \'" + "a" * 50 + "\'", use_cache=True)
        self.assertEqual(len(cache), 1)

        # Test that implicitly created long prepared statements
        # are not cached.
        await self.con.fetchval("SELECT \'" + "a" * 50 + "\'")
        self.assertEqual(len(cache), 1)

        # Test that short prepared statements can still be cached.
        await self.con._prepare('SELECT 2', use_cache=True)
        self.assertEqual(len(cache), 2)

    async def test_prepare_28_max_args(self):
        N = 32768
        args = ','.join('${}'.format(i) for i in range(1, N + 1))
        query = 'SELECT ARRAY[{}]'.format(args)

        with self.assertRaisesRegex(
                exceptions.InterfaceError,
                'the number of query arguments cannot exceed 32767'):
            await self.con.fetchval(query, *range(1, N + 1))

    async def test_prepare_29_duplicates(self):
        # In addition to test_record.py, let's have a full functional
        # test for records with duplicate keys.
        r = await self.con.fetchrow('SELECT 1 as a, 2 as b, 3 as a')
        self.assertEqual(list(r.items()), [('a', 1), ('b', 2), ('a', 3)])
        self.assertEqual(list(r.keys()), ['a', 'b', 'a'])
        self.assertEqual(list(r.values()), [1, 2, 3])
        self.assertEqual(r['a'], 3)
        self.assertEqual(r['b'], 2)
        self.assertEqual(r[0], 1)
        self.assertEqual(r[1], 2)
        self.assertEqual(r[2], 3)

    async def test_prepare_30_invalid_arg_count(self):
        with self.assertRaisesRegex(
                exceptions.InterfaceError,
                'the server expects 1 argument for this query, 0 were passed'):
            await self.con.fetchval('SELECT $1::int')

        with self.assertRaisesRegex(
                exceptions.InterfaceError,
                'the server expects 0 arguments for this query, 1 was passed'):
            await self.con.fetchval('SELECT 1', 1)

    async def test_prepare_31_pgbouncer_note(self):
        try:
            await self.con.execute("""
                DO $$ BEGIN
                    RAISE EXCEPTION
                        'duplicate statement' USING ERRCODE = '42P05';
                END; $$ LANGUAGE plpgsql;
            """)
        except asyncpg.DuplicatePreparedStatementError as e:
            self.assertTrue('pgbouncer' in e.hint)
        else:
            self.fail('DuplicatePreparedStatementError not raised')

        try:
            await self.con.execute("""
                DO $$ BEGIN
                    RAISE EXCEPTION
                        'invalid statement' USING ERRCODE = '26000';
                END; $$ LANGUAGE plpgsql;
            """)
        except asyncpg.InvalidSQLStatementNameError as e:
            self.assertTrue('pgbouncer' in e.hint)
        else:
            self.fail('InvalidSQLStatementNameError not raised')

    async def test_prepare_does_not_use_cache(self):
        cache = self.con._stmt_cache

        # prepare with disabled cache
        await self.con.prepare('select 1')
        self.assertEqual(len(cache), 0)
