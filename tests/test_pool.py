# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import platform
import os
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

        async with pool.acquire(timeout=POOL_NOMINAL_TIMEOUT):
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
            if con not in cons:
                raise RuntimeError('init was not called before setup')

        async def init(con):
            if con in cons:
                raise RuntimeError('init was called more than once')
            cons.add(con)

        async def user(pool):
            async with pool.acquire() as con:
                if con not in cons:
                    raise RuntimeError('init was not called')

        async with self.create_pool(database='postgres',
                                    min_size=2, max_size=5,
                                    init=init,
                                    setup=setup) as pool:
            users = asyncio.gather(*[user(pool) for _ in range(10)],
                                   loop=self.loop)
            await users

        self.assertEqual(len(cons), 5)

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
