# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import asyncpg

from asyncpg import _testbase as tb


class TestTimeout(tb.ConnectedTestCase):

    async def test_timeout_01(self):
        for methname in {'fetch', 'fetchrow', 'fetchval', 'execute'}:
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(0.1):
                meth = getattr(self.con, methname)
                await meth('select pg_sleep(10)', timeout=0.02)
            self.assertEqual(await self.con.fetch('select 1'), [(1,)])

    async def test_timeout_02(self):
        st = await self.con.prepare('select pg_sleep(10)')

        for methname in {'fetch', 'fetchrow', 'fetchval'}:
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(0.1):
                meth = getattr(st, methname)
                await meth(timeout=0.02)
            self.assertEqual(await self.con.fetch('select 1'), [(1,)])

    async def test_timeout_03(self):
        task = self.loop.create_task(
            self.con.fetch('select pg_sleep(10)', timeout=0.2))
        await asyncio.sleep(0.05, loop=self.loop)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError), \
                self.assertRunUnder(0.1):
            await task
        self.assertEqual(await self.con.fetch('select 1'), [(1,)])

    async def test_timeout_04(self):
        st = await self.con.prepare('select pg_sleep(10)', timeout=0.1)
        with self.assertRaises(asyncio.TimeoutError), \
                self.assertRunUnder(0.2):
            async with self.con.transaction():
                async for _ in st.cursor(timeout=0.1):  # NOQA
                    pass
        self.assertEqual(await self.con.fetch('select 1'), [(1,)])

        st = await self.con.prepare('select pg_sleep(10)', timeout=0.1)
        async with self.con.transaction():
            cur = await st.cursor()
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(0.2):
                await cur.fetch(1, timeout=0.1)
        self.assertEqual(await self.con.fetch('select 1'), [(1,)])

    async def test_timeout_05(self):
        # Stress-test timeouts - try to trigger a race condition
        # between a cancellation request to Postgres and next
        # query (SELECT 1)
        for _ in range(500):
            with self.assertRaises(asyncio.TimeoutError):
                await self.con.fetch('SELECT pg_sleep(1)', timeout=1e-10)
            self.assertEqual(await self.con.fetch('SELECT 1'), [(1,)])

    async def test_timeout_06(self):
        async with self.con.transaction():
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(0.2):
                async for _ in self.con.cursor(   # NOQA
                        'select pg_sleep(10)', timeout=0.1):
                    pass
        self.assertEqual(await self.con.fetch('select 1'), [(1,)])

        async with self.con.transaction():
            cur = await self.con.cursor('select pg_sleep(10)')
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(0.2):
                await cur.fetch(1, timeout=0.1)

        async with self.con.transaction():
            cur = await self.con.cursor('select pg_sleep(10)')
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(0.2):
                await cur.forward(1, timeout=1e-10)

        async with self.con.transaction():
            cur = await self.con.cursor('select pg_sleep(10)')
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(0.2):
                await cur.fetchrow(timeout=0.1)

        async with self.con.transaction():
            cur = await self.con.cursor('select pg_sleep(10)')
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(0.2):
                await cur.fetchrow(timeout=0.1)

            with self.assertRaises(asyncpg.InFailedSQLTransactionError):
                await cur.fetch(1)

        self.assertEqual(await self.con.fetch('select 1'), [(1,)])


class TestConnectionCommandTimeout(tb.ConnectedTestCase):

    def getExtraConnectOptions(self):
        return {
            'command_timeout': 0.02
        }

    async def test_command_timeout_01(self):
        for methname in {'fetch', 'fetchrow', 'fetchval', 'execute'}:
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(0.1):
                meth = getattr(self.con, methname)
                await meth('select pg_sleep(10)')
            self.assertEqual(await self.con.fetch('select 1'), [(1,)])
