# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import asyncpg

from asyncpg import _testbase as tb


class TestPool(tb.ClusterTestCase):

    async def test_pool_01(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                addr = self.cluster.get_connection_addr()
                pool = await asyncpg.create_pool(host=addr[0], port=addr[1],
                                                 database='postgres',
                                                 loop=self.loop, min_size=5,
                                                 max_size=10)

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
                addr = self.cluster.get_connection_addr()
                async with asyncpg.create_pool(host=addr[0], port=addr[1],
                                               database='postgres',
                                               loop=self.loop, min_size=5,
                                               max_size=5) as pool:

                    async def worker():
                        con = await pool.acquire(timeout=1)
                        self.assertEqual(await con.fetchval('SELECT 1'), 1)
                        await pool.release(con)

                    tasks = [worker() for _ in range(n)]
                    await asyncio.gather(*tasks, loop=self.loop)

    async def test_pool_03(self):
        addr = self.cluster.get_connection_addr()
        pool = await asyncpg.create_pool(host=addr[0], port=addr[1],
                                         database='postgres',
                                         loop=self.loop, min_size=1,
                                         max_size=1)

        con = await pool.acquire(timeout=1)
        with self.assertRaises(asyncio.TimeoutError):
            await pool.acquire(timeout=0.03)

        pool.terminate()
        del con
