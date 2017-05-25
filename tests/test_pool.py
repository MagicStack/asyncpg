# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import asyncpg
import inspect
import os
import platform
import random
import time
import unittest

from asyncpg import _testbase as tb
from asyncpg import connection as pg_connection
from asyncpg import cluster as pg_cluster
from asyncpg import pool as pg_pool

_system = platform.uname().system


if os.environ.get('TRAVIS_OS_NAME') == 'osx':
    # Travis' macOS is _slow_.
    POOL_NOMINAL_TIMEOUT = 0.5
else:
    POOL_NOMINAL_TIMEOUT = 0.1


class SlowResetConnection(pg_connection.Connection):
    """Connection class to simulate races with Connection.reset()."""
    async def reset(self):
        await asyncio.sleep(0.2, loop=self._loop)
        return await super().reset()


class SlowResetConnectionPool(pg_pool.Pool):
    async def _connect(self, *args, **kwargs):
        return await pg_connection.connect(
            *args, connection_class=SlowResetConnection, **kwargs)


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
                await asyncio.gather(*tasks, loop=self.loop)
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
                    await asyncio.gather(*tasks, loop=self.loop)

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
        con.terminate()
        await pool.release(con)

        async with pool.acquire(timeout=POOL_NOMINAL_TIMEOUT) as con:
            con.terminate()

        con = await pool.acquire(timeout=POOL_NOMINAL_TIMEOUT)
        self.assertEqual(await con.fetchval('SELECT 1'), 1)

        await pool.close()

    async def test_pool_05(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                pool = await self.create_pool(database='postgres',
                                              min_size=5, max_size=10)

                async def worker():
                    async with pool.acquire() as con:
                        self.assertEqual(await con.fetchval('SELECT 1'), 1)

                tasks = [worker() for _ in range(n)]
                await asyncio.gather(*tasks, loop=self.loop)
                await pool.close()

    async def test_pool_06(self):
        fut = asyncio.Future(loop=self.loop)

        async def setup(con):
            fut.set_result(con)

        async with self.create_pool(database='postgres',
                                    min_size=5, max_size=5,
                                    setup=setup) as pool:
            con = await pool.acquire()

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
            users = asyncio.gather(*[user(pool) for _ in range(10)],
                                   loop=self.loop)
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

        con = await pool1.acquire(timeout=POOL_NOMINAL_TIMEOUT)
        with self.assertRaisesRegex(asyncpg.InterfaceError, 'is not a member'):
            await pool2.release(con)

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

        self.assertIn('[released]', repr(con))

        with self.assertRaisesRegex(
                asyncpg.InterfaceError,
                r'cannot call Connection\.execute.*released back to the pool'):

            con.execute('select 1')

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

    async def test_pool_exception_in_setup_and_init(self):
        class Error(Exception):
            pass

        async def setup(con):
            nonlocal setup_calls
            setup_calls += 1
            if setup_calls > 1:
                cons.append(con)
            else:
                cons.append('error')
                raise Error

        with self.subTest(method='setup'):
            setup_calls = 0
            cons = []
            async with self.create_pool(database='postgres',
                                        min_size=1, max_size=1,
                                        setup=setup) as pool:
                with self.assertRaises(Error):
                    await pool.acquire()

                con = await pool.acquire()
                self.assertEqual(cons, ['error', con])

        with self.subTest(method='init'):
            setup_calls = 0
            cons = []
            async with self.create_pool(database='postgres',
                                        min_size=0, max_size=1,
                                        init=setup) as pool:
                with self.assertRaises(Error):
                    await pool.acquire()

                con = await pool.acquire()
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
            await asyncio.gather(*tasks, loop=self.loop)
            await pool.close()

        finally:
            await self.con.execute('DROP ROLE pooluser')

            # Reset cluster's pg_hba.conf since we've meddled with it
            self.cluster.trust_local_connections()
            self.cluster.reload()

    async def test_pool_handles_cancel_in_release(self):
        # Use SlowResetConnectionPool to simulate
        # the Task.cancel() and __aexit__ race.
        pool = await self.create_pool(database='postgres',
                                      min_size=1, max_size=1,
                                      pool_class=SlowResetConnectionPool)

        async def worker():
            async with pool.acquire():
                pass

        task = self.loop.create_task(worker())
        # Let the worker() run.
        await asyncio.sleep(0.1, loop=self.loop)
        # Cancel the worker.
        task.cancel()
        # Wait to make sure the cleanup has completed.
        await asyncio.sleep(0.4, loop=self.loop)
        # Check that the connection has been returned to the pool.
        self.assertEqual(pool._queue.qsize(), 1)

    async def test_pool_no_acquire_deadlock(self):
        async with self.create_pool(database='postgres',
                                    min_size=1, max_size=1,
                                    max_queries=1) as pool:

            async def sleep_and_release():
                async with pool.acquire() as con:
                    await con.execute('SELECT pg_sleep(1)')

            asyncio.ensure_future(sleep_and_release(), loop=self.loop)
            await asyncio.sleep(0.5, loop=self.loop)

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

            await asyncio.gather(*[test(pool) for _ in range(N)],
                                 loop=self.loop)

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
            await asyncio.sleep(random.random() / 100, loop=self.loop)
            r = await pool.fetch('SELECT {}::int'.format(i))
            self.assertEqual(r, [(i,)])
            return 1

        async def test_fetchrow(pool):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100, loop=self.loop)
            r = await pool.fetchrow('SELECT {}::int'.format(i))
            self.assertEqual(r, (i,))
            return 1

        async def test_fetchval(pool):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100, loop=self.loop)
            r = await pool.fetchval('SELECT {}::int'.format(i))
            self.assertEqual(r, i)
            return 1

        async def test_execute(pool):
            await asyncio.sleep(random.random() / 100, loop=self.loop)
            r = await pool.execute('SELECT generate_series(0, 10)')
            self.assertEqual(r, 'SELECT {}'.format(11))
            return 1

        async def test_execute_with_arg(pool):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100, loop=self.loop)
            r = await pool.execute('SELECT generate_series(0, $1)', i)
            self.assertEqual(r, 'SELECT {}'.format(i + 1))
            return 1

        async def run(N, meth):
            async with self.create_pool(database='postgres',
                                        min_size=5, max_size=10) as pool:

                coros = [meth(pool) for _ in range(N)]
                res = await asyncio.gather(*coros, loop=self.loop)
                self.assertEqual(res, [1] * N)

        methods = [test_fetch, test_fetchrow, test_fetchval,
                   test_execute, test_execute_with_arg]

        for method in methods:
            with self.subTest(method=method.__name__):
                await run(200, method)

    async def test_pool_connection_execute_many(self):
        async def worker(pool):
            await asyncio.sleep(random.random() / 100, loop=self.loop)
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
                res = await asyncio.gather(*coros, loop=self.loop)
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

            await asyncio.sleep(1, loop=self.loop)
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
            await asyncio.sleep(0.6, loop=self.loop)

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
            await asyncio.sleep(random.random() / 10 + 0.1, loop=self.loop)
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
            await asyncio.gather(*workers, loop=self.loop)

        self.assertGreaterEqual(N, 50)


@unittest.skipIf(os.environ.get('PGHOST'), 'using remote cluster for testing')
class TestHostStandby(tb.ConnectedTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.master_cluster = cls.start_cluster(
            pg_cluster.TempCluster,
            server_settings={
                'max_wal_senders': 10,
                'wal_level': 'hot_standby'
            })

        con = None

        try:
            con = cls.loop.run_until_complete(
                cls.master_cluster.connect(database='postgres', loop=cls.loop))

            cls.loop.run_until_complete(
                con.execute('''
                    CREATE ROLE replication WITH LOGIN REPLICATION
                '''))

            cls.master_cluster.trust_local_replication_by('replication')

            conn_spec = cls.master_cluster.get_connection_spec()

            cls.standby_cluster = cls.start_cluster(
                pg_cluster.HotStandbyCluster,
                cluster_kwargs={
                    'master': conn_spec,
                    'replication_user': 'replication'
                },
                server_settings={
                    'hot_standby': True
                })

        finally:
            if con is not None:
                cls.loop.run_until_complete(con.close())

    @classmethod
    def tearDownMethod(cls):
        cls.standby_cluster.stop()
        cls.standby_cluster.destroy()
        cls.master_cluster.stop()
        cls.master_cluster.destroy()

    def create_pool(self, **kwargs):
        conn_spec = self.standby_cluster.get_connection_spec()
        conn_spec.update(kwargs)
        return pg_pool.create_pool(loop=self.loop, **conn_spec)

    async def test_standby_pool_01(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                pool = await self.create_pool(database='postgres',
                                              min_size=5, max_size=10)

                async def worker():
                    con = await pool.acquire()
                    self.assertEqual(await con.fetchval('SELECT 1'), 1)
                    await pool.release(con)

                tasks = [worker() for _ in range(n)]
                await asyncio.gather(*tasks, loop=self.loop)
                await pool.close()
