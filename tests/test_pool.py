# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import inspect
import os
import platform
import random
import sys
import textwrap
import time
import unittest

import asyncpg
from asyncpg import _testbase as tb
from asyncpg import connection as pg_connection
from asyncpg import cluster as pg_cluster
from asyncpg import pool as pg_pool

_system = platform.uname().system


POOL_NOMINAL_TIMEOUT = 0.1


class SlowResetConnection(pg_connection.Connection):
    """Connection class to simulate races with Connection.reset()."""
    async def reset(self, *, timeout=None):
        await asyncio.sleep(0.2)
        return await super().reset(timeout=timeout)


class SlowCancelConnection(pg_connection.Connection):
    """Connection class to simulate races with Connection._cancel()."""
    async def _cancel(self, waiter):
        await asyncio.sleep(0.2)
        return await super()._cancel(waiter)


class TestPool(tb.ConnectedTestCase):

    async def test_pool_01(self):
        for n in {1, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                pool = await self.create_pool(database='postgres',
                                              min_size=5, max_size=10)

                async def worker():
                    con = await pool.acquire()
                    self.assertEqual(await con.fetchval('SELECT 1'), 1)
                    await pool.release(con)

                tasks = [worker() for _ in range(n)]
                await asyncio.gather(*tasks)
                await pool.close()

    async def test_pool_02(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                async with self.create_pool(database='postgres',
                                            min_size=5, max_size=5) as pool:

                    async def worker():
                        con = await pool.acquire(timeout=5)
                        self.assertEqual(await con.fetchval('SELECT 1'), 1)
                        await pool.release(con)

                    tasks = [worker() for _ in range(n)]
                    await asyncio.gather(*tasks)

    async def test_pool_03(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        con = await pool.acquire(timeout=1)
        with self.assertRaises(asyncio.TimeoutError):
            await pool.acquire(timeout=0.03)

        pool.terminate()
        del con

    async def test_pool_04(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        con = await pool.acquire(timeout=POOL_NOMINAL_TIMEOUT)

        # Manual termination of pool connections releases the
        # pool item immediately.
        con.terminate()
        self.assertIsNone(pool._holders[0]._con)
        self.assertIsNone(pool._holders[0]._in_use)

        con = await pool.acquire(timeout=POOL_NOMINAL_TIMEOUT)
        self.assertEqual(await con.fetchval('SELECT 1'), 1)

        await con.close()
        self.assertIsNone(pool._holders[0]._con)
        self.assertIsNone(pool._holders[0]._in_use)
        # Calling release should not hurt.
        await pool.release(con)

        pool.terminate()

    async def test_pool_05(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                pool = await self.create_pool(database='postgres',
                                              min_size=5, max_size=10)

                async def worker():
                    async with pool.acquire() as con:
                        self.assertEqual(await con.fetchval('SELECT 1'), 1)

                tasks = [worker() for _ in range(n)]
                await asyncio.gather(*tasks)
                await pool.close()

    async def test_pool_06(self):
        fut = asyncio.Future()

        async def setup(con):
            fut.set_result(con)

        async with self.create_pool(database='postgres',
                                    min_size=5, max_size=5,
                                    setup=setup) as pool:
            async with pool.acquire() as con:
                pass

        self.assertIs(con, await fut)

    async def test_pool_07(self):
        cons = set()

        async def setup(con):
            if con._con not in cons:  # `con` is `PoolConnectionProxy`.
                raise RuntimeError('init was not called before setup')

        async def init(con):
            if con in cons:
                raise RuntimeError('init was called more than once')
            cons.add(con)

        async def user(pool):
            async with pool.acquire() as con:
                if con._con not in cons:  # `con` is `PoolConnectionProxy`.
                    raise RuntimeError('init was not called')

        async with self.create_pool(database='postgres',
                                    min_size=2, max_size=5,
                                    init=init,
                                    setup=setup) as pool:
            users = asyncio.gather(*[user(pool) for _ in range(10)])
            await users

        self.assertEqual(len(cons), 5)

    async def test_pool_08(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        con = await pool.acquire(timeout=POOL_NOMINAL_TIMEOUT)
        with self.assertRaisesRegex(asyncpg.InterfaceError, 'is not a member'):
            await pool.release(con._con)

    async def test_pool_09(self):
        pool1 = await self.create_pool(database='postgres',
                                       min_size=1, max_size=1)

        pool2 = await self.create_pool(database='postgres',
                                       min_size=1, max_size=1)

        try:
            con = await pool1.acquire(timeout=POOL_NOMINAL_TIMEOUT)
            with self.assertRaisesRegex(asyncpg.InterfaceError,
                                        'is not a member'):
                await pool2.release(con)
        finally:
            await pool1.release(con)

        await pool1.close()
        await pool2.close()

    async def test_pool_10(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        con = await pool.acquire()
        await pool.release(con)
        await pool.release(con)

        await pool.close()

    async def test_pool_11(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        async with pool.acquire() as con:
            self.assertIn(repr(con._con), repr(con))  # Test __repr__.

            ps = await con.prepare('SELECT 1')
            txn = con.transaction()
            async with con.transaction():
                cur = await con.cursor('SELECT 1')
                ps_cur = await ps.cursor()

        self.assertIn('[released]', repr(con))

        with self.assertRaisesRegex(
                asyncpg.InterfaceError,
                r'cannot call Connection\.execute.*released back to the pool'):

            con.execute('select 1')

        for meth in ('fetchval', 'fetchrow', 'fetch', 'explain',
                     'get_query', 'get_statusmsg', 'get_parameters',
                     'get_attributes'):
            with self.assertRaisesRegex(
                    asyncpg.InterfaceError,
                    r'cannot call PreparedStatement\.{meth}.*released '
                    r'back to the pool'.format(meth=meth)):

                getattr(ps, meth)()

        for c in (cur, ps_cur):
            for meth in ('fetch', 'fetchrow'):
                with self.assertRaisesRegex(
                        asyncpg.InterfaceError,
                        r'cannot call Cursor\.{meth}.*released '
                        r'back to the pool'.format(meth=meth)):

                    getattr(c, meth)()

            with self.assertRaisesRegex(
                    asyncpg.InterfaceError,
                    r'cannot call Cursor\.forward.*released '
                    r'back to the pool'.format(meth=meth)):

                c.forward(1)

        for meth in ('start', 'commit', 'rollback'):
            with self.assertRaisesRegex(
                    asyncpg.InterfaceError,
                    r'cannot call Transaction\.{meth}.*released '
                    r'back to the pool'.format(meth=meth)):

                getattr(txn, meth)()

        await pool.close()

    async def test_pool_12(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        async with pool.acquire() as con:
            self.assertTrue(isinstance(con, pg_connection.Connection))
            self.assertFalse(isinstance(con, list))

        await pool.close()

    async def test_pool_13(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        async with pool.acquire() as con:
            self.assertIn('Execute an SQL command', con.execute.__doc__)
            self.assertEqual(con.execute.__name__, 'execute')

            self.assertIn(
                str(inspect.signature(con.execute))[1:],
                str(inspect.signature(pg_connection.Connection.execute)))

        await pool.close()

    def test_pool_init_run_until_complete(self):
        pool_init = self.create_pool(database='postgres')
        pool = self.loop.run_until_complete(pool_init)
        self.assertIsInstance(pool, asyncpg.pool.Pool)

    async def test_pool_exception_in_setup_and_init(self):
        class Error(Exception):
            pass

        async def setup(con):
            nonlocal setup_calls, last_con
            last_con = con
            setup_calls += 1
            if setup_calls > 1:
                cons.append(con)
            else:
                cons.append('error')
                raise Error

        with self.subTest(method='setup'):
            setup_calls = 0
            last_con = None
            cons = []
            async with self.create_pool(database='postgres',
                                        min_size=1, max_size=1,
                                        setup=setup) as pool:
                with self.assertRaises(Error):
                    await pool.acquire()
                self.assertTrue(last_con.is_closed())

                async with pool.acquire() as con:
                    self.assertEqual(cons, ['error', con])

        with self.subTest(method='init'):
            setup_calls = 0
            last_con = None
            cons = []
            async with self.create_pool(database='postgres',
                                        min_size=0, max_size=1,
                                        init=setup) as pool:
                with self.assertRaises(Error):
                    await pool.acquire()
                self.assertTrue(last_con.is_closed())

                async with pool.acquire() as con:
                    self.assertEqual(await con.fetchval('select 1::int'), 1)
                    self.assertEqual(cons, ['error', con._con])

    async def test_pool_auth(self):
        if not self.cluster.is_managed():
            self.skipTest('unmanaged cluster')

        self.cluster.reset_hba()

        if _system != 'Windows':
            self.cluster.add_hba_entry(
                type='local',
                database='postgres', user='pooluser',
                auth_method='md5')

        self.cluster.add_hba_entry(
            type='host', address='127.0.0.1/32',
            database='postgres', user='pooluser',
            auth_method='md5')

        self.cluster.add_hba_entry(
            type='host', address='::1/128',
            database='postgres', user='pooluser',
            auth_method='md5')

        self.cluster.reload()

        try:
            await self.con.execute('''
                CREATE ROLE pooluser WITH LOGIN PASSWORD 'poolpassword'
            ''')

            pool = await self.create_pool(database='postgres',
                                          user='pooluser',
                                          password='poolpassword',
                                          min_size=5, max_size=10)

            async def worker():
                con = await pool.acquire()
                self.assertEqual(await con.fetchval('SELECT 1'), 1)
                await pool.release(con)

            tasks = [worker() for _ in range(5)]
            await asyncio.gather(*tasks)
            await pool.close()

        finally:
            await self.con.execute('DROP ROLE pooluser')

            # Reset cluster's pg_hba.conf since we've meddled with it
            self.cluster.trust_local_connections()
            self.cluster.reload()

    async def test_pool_handles_task_cancel_in_acquire_with_timeout(self):
        # See https://github.com/MagicStack/asyncpg/issues/547
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        async def worker():
            async with pool.acquire(timeout=100):
                pass

        # Schedule task
        task = self.loop.create_task(worker())
        # Yield to task, but cancel almost immediately
        await asyncio.sleep(0.00000000001)
        # Cancel the worker.
        task.cancel()
        # Wait to make sure the cleanup has completed.
        await asyncio.sleep(0.4)
        # Check that the connection has been returned to the pool.
        self.assertEqual(pool._queue.qsize(), 1)

    async def test_pool_handles_task_cancel_in_release(self):
        # Use SlowResetConnectionPool to simulate
        # the Task.cancel() and __aexit__ race.
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1,
                                      connection_class=SlowResetConnection)

        async def worker():
            async with pool.acquire():
                pass

        task = self.loop.create_task(worker())
        # Let the worker() run.
        await asyncio.sleep(0.1)
        # Cancel the worker.
        task.cancel()
        # Wait to make sure the cleanup has completed.
        await asyncio.sleep(0.4)
        # Check that the connection has been returned to the pool.
        self.assertEqual(pool._queue.qsize(), 1)

    async def test_pool_handles_query_cancel_in_release(self):
        # Use SlowResetConnectionPool to simulate
        # the Task.cancel() and __aexit__ race.
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1,
                                      connection_class=SlowCancelConnection)

        async def worker():
            async with pool.acquire() as con:
                await con.execute('SELECT pg_sleep(10)')

        task = self.loop.create_task(worker())
        # Let the worker() run.
        await asyncio.sleep(0.1)
        # Cancel the worker.
        task.cancel()
        # Wait to make sure the cleanup has completed.
        await asyncio.sleep(0.5)
        # Check that the connection has been returned to the pool.
        self.assertEqual(pool._queue.qsize(), 1)

    async def test_pool_no_acquire_deadlock(self):
        async with self.create_pool(database='postgres',
                                    min_size=1, max_size=1,
                                    max_queries=1) as pool:

            async def sleep_and_release():
                async with pool.acquire() as con:
                    await con.execute('SELECT pg_sleep(1)')

            asyncio.ensure_future(sleep_and_release())
            await asyncio.sleep(0.5)

            async with pool.acquire() as con:
                await con.fetchval('SELECT 1')

    async def test_pool_config_persistence(self):
        N = 100
        cons = set()

        class MyConnection(asyncpg.Connection):
            async def foo(self):
                return 42

            async def fetchval(self, query):
                res = await super().fetchval(query)
                return res + 1

        async def test(pool):
            async with pool.acquire() as con:
                self.assertEqual(await con.fetchval('SELECT 1'), 2)
                self.assertEqual(await con.foo(), 42)
                self.assertTrue(isinstance(con, MyConnection))
                self.assertEqual(con._con._config.statement_cache_size, 3)
                cons.add(con)

        async with self.create_pool(
                database='postgres', min_size=10, max_size=10,
                max_queries=1, connection_class=MyConnection,
                statement_cache_size=3) as pool:

            await asyncio.gather(*[test(pool) for _ in range(N)])

        self.assertEqual(len(cons), N)

    async def test_pool_release_in_xact(self):
        """Test that Connection.reset() closes any open transaction."""
        async with self.create_pool(database='postgres',
                                    min_size=1, max_size=1) as pool:
            async def get_xact_id(con):
                return await con.fetchval('select txid_current()')

            with self.assertLoopErrorHandlerCalled('an active transaction'):
                async with pool.acquire() as con:
                    real_con = con._con  # unwrap PoolConnectionProxy

                    id1 = await get_xact_id(con)

                    tr = con.transaction()
                    self.assertIsNone(con._con._top_xact)
                    await tr.start()
                    self.assertIs(real_con._top_xact, tr)

                    id2 = await get_xact_id(con)
                    self.assertNotEqual(id1, id2)

            self.assertIsNone(real_con._top_xact)

            async with pool.acquire() as con:
                self.assertIs(con._con, real_con)
                self.assertIsNone(con._con._top_xact)
                id3 = await get_xact_id(con)
                self.assertNotEqual(id2, id3)

    async def test_pool_connection_methods(self):
        async def test_fetch(pool):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100)
            r = await pool.fetch('SELECT {}::int'.format(i))
            self.assertEqual(r, [(i,)])
            return 1

        async def test_fetchrow(pool):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100)
            r = await pool.fetchrow('SELECT {}::int'.format(i))
            self.assertEqual(r, (i,))
            return 1

        async def test_fetchval(pool):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100)
            r = await pool.fetchval('SELECT {}::int'.format(i))
            self.assertEqual(r, i)
            return 1

        async def test_execute(pool):
            await asyncio.sleep(random.random() / 100)
            r = await pool.execute('SELECT generate_series(0, 10)')
            self.assertEqual(r, 'SELECT {}'.format(11))
            return 1

        async def test_execute_with_arg(pool):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100)
            r = await pool.execute('SELECT generate_series(0, $1)', i)
            self.assertEqual(r, 'SELECT {}'.format(i + 1))
            return 1

        async def run(N, meth):
            async with self.create_pool(database='postgres',
                                        min_size=5, max_size=10) as pool:

                coros = [meth(pool) for _ in range(N)]
                res = await asyncio.gather(*coros)
                self.assertEqual(res, [1] * N)

        methods = [test_fetch, test_fetchrow, test_fetchval,
                   test_execute, test_execute_with_arg]

        with tb.silence_asyncio_long_exec_warning():
            for method in methods:
                with self.subTest(method=method.__name__):
                    await run(200, method)

    async def test_pool_connection_execute_many(self):
        async def worker(pool):
            await asyncio.sleep(random.random() / 100)
            await pool.executemany('''
                INSERT INTO exmany VALUES($1, $2)
            ''', [
                ('a', 1), ('b', 2), ('c', 3), ('d', 4)
            ])
            return 1

        N = 200

        async with self.create_pool(database='postgres',
                                    min_size=5, max_size=10) as pool:

            await pool.execute('CREATE TABLE exmany (a text, b int)')
            try:

                coros = [worker(pool) for _ in range(N)]
                res = await asyncio.gather(*coros)
                self.assertEqual(res, [1] * N)

                n_rows = await pool.fetchval('SELECT count(*) FROM exmany')
                self.assertEqual(n_rows, N * 4)

            finally:
                await pool.execute('DROP TABLE exmany')

    async def test_pool_max_inactive_time_01(self):
        async with self.create_pool(
                database='postgres', min_size=1, max_size=1,
                max_inactive_connection_lifetime=0.1) as pool:

            # Test that it's OK if a query takes longer time to execute
            # than `max_inactive_connection_lifetime`.

            con = pool._holders[0]._con

            for _ in range(3):
                await pool.execute('SELECT pg_sleep(0.5)')
                self.assertIs(pool._holders[0]._con, con)

                self.assertEqual(
                    await pool.execute('SELECT 1::int'),
                    'SELECT 1')
                self.assertIs(pool._holders[0]._con, con)

    async def test_pool_max_inactive_time_02(self):
        async with self.create_pool(
                database='postgres', min_size=1, max_size=1,
                max_inactive_connection_lifetime=0.5) as pool:

            # Test that we have a new connection after pool not
            # being used longer than `max_inactive_connection_lifetime`.

            con = pool._holders[0]._con

            self.assertEqual(
                await pool.execute('SELECT 1::int'),
                'SELECT 1')
            self.assertIs(pool._holders[0]._con, con)

            await asyncio.sleep(1)
            self.assertIs(pool._holders[0]._con, None)

            self.assertEqual(
                await pool.execute('SELECT 1::int'),
                'SELECT 1')
            self.assertIsNot(pool._holders[0]._con, con)

    async def test_pool_max_inactive_time_03(self):
        async with self.create_pool(
                database='postgres', min_size=1, max_size=1,
                max_inactive_connection_lifetime=1) as pool:

            # Test that we start counting inactive time *after*
            # the connection is being released back to the pool.

            con = pool._holders[0]._con

            await pool.execute('SELECT pg_sleep(0.5)')
            await asyncio.sleep(0.6)

            self.assertIs(pool._holders[0]._con, con)

            self.assertEqual(
                await pool.execute('SELECT 1::int'),
                'SELECT 1')
            self.assertIs(pool._holders[0]._con, con)

    async def test_pool_max_inactive_time_04(self):
        # Chaos test for max_inactive_connection_lifetime.
        DURATION = 2.0
        START = time.monotonic()
        N = 0

        async def worker(pool):
            nonlocal N
            await asyncio.sleep(random.random() / 10 + 0.1)
            async with pool.acquire() as con:
                if random.random() > 0.5:
                    await con.execute('SELECT pg_sleep({:.2f})'.format(
                        random.random() / 10))
                self.assertEqual(
                    await con.fetchval('SELECT 42::int'),
                    42)

            if time.monotonic() - START < DURATION:
                await worker(pool)

            N += 1

        async with self.create_pool(
                database='postgres', min_size=10, max_size=30,
                max_inactive_connection_lifetime=0.1) as pool:

            workers = [worker(pool) for _ in range(50)]
            await asyncio.gather(*workers)

        self.assertGreaterEqual(N, 50)

    async def test_pool_max_inactive_time_05(self):
        # Test that idle never-acquired connections abide by
        # the max inactive lifetime.
        async with self.create_pool(
                database='postgres', min_size=2, max_size=2,
                max_inactive_connection_lifetime=0.2) as pool:

            self.assertIsNotNone(pool._holders[0]._con)
            self.assertIsNotNone(pool._holders[1]._con)

            await pool.execute('SELECT pg_sleep(0.3)')
            await asyncio.sleep(0.3)

            self.assertIs(pool._holders[0]._con, None)
            # The connection in the second holder was never used,
            # but should be closed nonetheless.
            self.assertIs(pool._holders[1]._con, None)

    async def test_pool_handles_inactive_connection_errors(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        con = await pool.acquire(timeout=POOL_NOMINAL_TIMEOUT)

        true_con = con._con

        await pool.release(con)

        # we simulate network error by terminating the connection
        true_con.terminate()

        # now pool should reopen terminated connection
        async with pool.acquire(timeout=POOL_NOMINAL_TIMEOUT) as con:
            self.assertEqual(await con.fetchval('SELECT 1'), 1)
            await con.close()

        await pool.close()

    @unittest.skipIf(sys.version_info[:2] < (3, 6), 'no asyncgen support')
    async def test_pool_handles_transaction_exit_in_asyncgen_1(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        locals_ = {}
        exec(textwrap.dedent('''\
            async def iterate(con):
                async with con.transaction():
                    for record in await con.fetch("SELECT 1"):
                        yield record
        '''), globals(), locals_)
        iterate = locals_['iterate']

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            async with pool.acquire() as con:
                async for _ in iterate(con):  # noqa
                    raise MyException()

    @unittest.skipIf(sys.version_info[:2] < (3, 6), 'no asyncgen support')
    async def test_pool_handles_transaction_exit_in_asyncgen_2(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        locals_ = {}
        exec(textwrap.dedent('''\
            async def iterate(con):
                async with con.transaction():
                    for record in await con.fetch("SELECT 1"):
                        yield record
        '''), globals(), locals_)
        iterate = locals_['iterate']

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            async with pool.acquire() as con:
                iterator = iterate(con)
                async for _ in iterator:  # noqa
                    raise MyException()

            del iterator

    @unittest.skipIf(sys.version_info[:2] < (3, 6), 'no asyncgen support')
    async def test_pool_handles_asyncgen_finalization(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        locals_ = {}
        exec(textwrap.dedent('''\
            async def iterate(con):
                for record in await con.fetch("SELECT 1"):
                    yield record
        '''), globals(), locals_)
        iterate = locals_['iterate']

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            async with pool.acquire() as con:
                async with con.transaction():
                    async for _ in iterate(con):  # noqa
                        raise MyException()

    async def test_pool_close_waits_for_release(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        flag = self.loop.create_future()
        conn_released = False

        async def worker():
            nonlocal conn_released

            async with pool.acquire() as connection:
                async with connection.transaction():
                    flag.set_result(True)
                    await asyncio.sleep(0.1)

            conn_released = True

        self.loop.create_task(worker())

        await flag
        await pool.close()
        self.assertTrue(conn_released)

    async def test_pool_close_timeout(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        flag = self.loop.create_future()

        async def worker():
            async with pool.acquire():
                flag.set_result(True)
                await asyncio.sleep(0.5)

        task = self.loop.create_task(worker())

        with self.assertRaises(asyncio.TimeoutError):
            await flag
            await asyncio.wait_for(pool.close(), timeout=0.1)

        await task

    async def test_pool_expire_connections(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        con = await pool.acquire()
        try:
            await pool.expire_connections()
        finally:
            await pool.release(con)

        self.assertIsNone(pool._holders[0]._con)

    async def test_pool_set_connection_args(self):
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1)

        # Test that connection is expired on release.
        con = await pool.acquire()
        connspec = self.get_connection_spec()
        try:
            connspec['server_settings']['application_name'] = \
                'set_conn_args_test'
        except KeyError:
            connspec['server_settings'] = {
                'application_name': 'set_conn_args_test'
            }

        pool.set_connect_args(**connspec)
        await pool.expire_connections()
        await pool.release(con)

        con = await pool.acquire()
        self.assertEqual(con.get_settings().application_name,
                         'set_conn_args_test')
        await pool.release(con)

        # Test that connection is expired before acquire.
        connspec = self.get_connection_spec()
        try:
            connspec['server_settings']['application_name'] = \
                'set_conn_args_test'
        except KeyError:
            connspec['server_settings'] = {
                'application_name': 'set_conn_args_test_2'
            }

        pool.set_connect_args(**connspec)
        await pool.expire_connections()

        con = await pool.acquire()
        self.assertEqual(con.get_settings().application_name,
                         'set_conn_args_test_2')

    async def test_pool_init_race(self):
        pool = self.create_pool(database='postgres', min_size=1, max_size=1)

        t1 = asyncio.ensure_future(pool)
        t2 = asyncio.ensure_future(pool)

        await t1
        with self.assertRaisesRegex(
                asyncpg.InterfaceError,
                r'pool is being initialized in another task'):
            await t2

        await pool.close()

    async def test_pool_init_and_use_race(self):
        pool = self.create_pool(database='postgres', min_size=1, max_size=1)

        pool_task = asyncio.ensure_future(pool)
        await asyncio.sleep(0)

        with self.assertRaisesRegex(
                asyncpg.InterfaceError,
                r'being initialized, but not yet ready'):

            await pool.fetchval('SELECT 1')

        await pool_task
        await pool.close()

    async def test_pool_remote_close(self):
        pool = await self.create_pool(min_size=1, max_size=1)
        backend_pid_fut = self.loop.create_future()

        async def worker():
            async with pool.acquire() as conn:
                pool_backend_pid = await conn.fetchval(
                    'SELECT pg_backend_pid()')
                backend_pid_fut.set_result(pool_backend_pid)
                await asyncio.sleep(0.2)

        task = self.loop.create_task(worker())
        try:
            conn = await self.connect()
            backend_pid = await backend_pid_fut
            await conn.execute('SELECT pg_terminate_backend($1)', backend_pid)
        finally:
            await conn.close()

        await task

        # Check that connection_lost has released the pool holder.
        conn = await pool.acquire(timeout=0.1)
        await pool.release(conn)


@unittest.skipIf(os.environ.get('PGHOST'), 'using remote cluster for testing')
class TestHotStandby(tb.ClusterTestCase):
    @classmethod
    def setup_cluster(cls):
        cls.master_cluster = cls.new_cluster(pg_cluster.TempCluster)
        cls.start_cluster(
            cls.master_cluster,
            server_settings={
                'max_wal_senders': 10,
                'wal_level': 'hot_standby'
            }
        )

        con = None

        try:
            con = cls.loop.run_until_complete(
                cls.master_cluster.connect(
                    database='postgres', user='postgres', loop=cls.loop))

            cls.loop.run_until_complete(
                con.execute('''
                    CREATE ROLE replication WITH LOGIN REPLICATION
                '''))

            cls.master_cluster.trust_local_replication_by('replication')

            conn_spec = cls.master_cluster.get_connection_spec()

            cls.standby_cluster = cls.new_cluster(
                pg_cluster.HotStandbyCluster,
                cluster_kwargs={
                    'master': conn_spec,
                    'replication_user': 'replication'
                }
            )
            cls.start_cluster(
                cls.standby_cluster,
                server_settings={
                    'hot_standby': True
                }
            )

        finally:
            if con is not None:
                cls.loop.run_until_complete(con.close())

    def create_pool(self, **kwargs):
        conn_spec = self.standby_cluster.get_connection_spec()
        conn_spec.update(kwargs)
        return pg_pool.create_pool(loop=self.loop, **conn_spec)

    async def test_standby_pool_01(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                pool = await self.create_pool(
                    database='postgres', user='postgres',
                    min_size=5, max_size=10)

                async def worker():
                    con = await pool.acquire()
                    self.assertEqual(await con.fetchval('SELECT 1'), 1)
                    await pool.release(con)

                tasks = [worker() for _ in range(n)]
                await asyncio.gather(*tasks)
                await pool.close()

    async def test_standby_cursors(self):
        con = await self.standby_cluster.connect(
            database='postgres', user='postgres', loop=self.loop)

        try:
            async with con.transaction():
                cursor = await con.cursor('SELECT 1')
                self.assertEqual(await cursor.fetchrow(), (1,))
        finally:
            await con.close()
