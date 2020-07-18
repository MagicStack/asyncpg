# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import asyncpg

from asyncpg import _testbase as tb


class TestCancellation(tb.ConnectedTestCase):

    async def test_cancellation_01(self):
        st1000 = await self.con.prepare('SELECT 1000')

        async def test0():
            val = await self.con.execute('SELECT 42')
            self.assertEqual(val, 'SELECT 1')

        async def test1():
            val = await self.con.fetchval('SELECT 42')
            self.assertEqual(val, 42)

        async def test2():
            val = await self.con.fetchrow('SELECT 42')
            self.assertEqual(val, (42,))

        async def test3():
            val = await self.con.fetch('SELECT 42')
            self.assertEqual(val, [(42,)])

        async def test4():
            val = await self.con.prepare('SELECT 42')
            self.assertEqual(await val.fetchval(), 42)

        async def test5():
            self.assertEqual(await st1000.fetchval(), 1000)

        async def test6():
            self.assertEqual(await st1000.fetchrow(), (1000,))

        async def test7():
            self.assertEqual(await st1000.fetch(), [(1000,)])

        async def test8():
            cur = await st1000.cursor()
            self.assertEqual(await cur.fetchrow(), (1000,))

        for test in {test0, test1, test2, test3, test4, test5,
                     test6, test7, test8}:

            with self.subTest(testfunc=test), self.assertRunUnder(1):
                st = await self.con.prepare('SELECT pg_sleep(20)')
                task = self.loop.create_task(st.fetch())
                await asyncio.sleep(0.05)
                task.cancel()

                with self.assertRaises(asyncio.CancelledError):
                    await task

                async with self.con.transaction():
                    await test()

    async def test_cancellation_02(self):
        st = await self.con.prepare('SELECT 1')
        task = self.loop.create_task(st.fetch())
        await asyncio.sleep(0.05)
        task.cancel()
        self.assertEqual(await task, [(1,)])

    async def test_cancellation_03(self):
        with self.assertRaises(asyncpg.InFailedSQLTransactionError):
            async with self.con.transaction():
                task = self.loop.create_task(
                    self.con.fetch('SELECT pg_sleep(20)'))
                await asyncio.sleep(0.05)
                task.cancel()

                with self.assertRaises(asyncio.CancelledError):
                    await task

                await self.con.fetch('SELECT generate_series(0, 100)')

        self.assertEqual(
            await self.con.fetchval('SELECT 42'),
            42)

    async def test_cancellation_04(self):
        await self.con.fetchval('SELECT pg_sleep(0)')
        waiter = asyncio.Future()
        self.con._cancel_current_command(waiter)
        await waiter
        self.assertEqual(await self.con.fetchval('SELECT 42'), 42)
