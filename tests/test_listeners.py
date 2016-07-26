# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio

from asyncpg import _testbase as tb


class TestListeners(tb.ClusterTestCase):

    async def test_listen_01(self):
        async with self.create_pool(database='postgres') as pool:
            async with pool.acquire() as con:

                q1 = asyncio.Queue(loop=self.loop)
                q2 = asyncio.Queue(loop=self.loop)

                def listener1(*args):
                    q1.put_nowait(args)

                def listener2(*args):
                    q2.put_nowait(args)

                await con.add_listener('test', listener1)
                await con.add_listener('test', listener2)

                await con.execute("NOTIFY test, 'aaaa'")

                self.assertEqual(
                    await q1.get(),
                    (con, con.get_server_pid(), 'test', 'aaaa'))
                self.assertEqual(
                    await q2.get(),
                    (con, con.get_server_pid(), 'test', 'aaaa'))

                await con.remove_listener('test', listener2)

                await con.execute("NOTIFY test, 'aaaa'")

                self.assertEqual(
                    await q1.get(),
                    (con, con.get_server_pid(), 'test', 'aaaa'))
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(q2.get(),
                                           timeout=0.05, loop=self.loop)

                await con.reset()
                await con.remove_listener('test', listener1)
                await con.execute("NOTIFY test, 'aaaa'")

                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(q1.get(),
                                           timeout=0.05, loop=self.loop)
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(q2.get(),
                                           timeout=0.05, loop=self.loop)

    async def test_listen_02(self):
        async with self.create_pool(database='postgres') as pool:
            async with pool.acquire() as con1, pool.acquire() as con2:

                q1 = asyncio.Queue(loop=self.loop)

                def listener1(*args):
                    q1.put_nowait(args)

                await con1.add_listener('ipc', listener1)
                await con2.execute("NOTIFY ipc, 'hello'")

                self.assertEqual(
                    await q1.get(),
                    (con1, con2.get_server_pid(), 'ipc', 'hello'))
