# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio

import asyncpg
from asyncpg import connection as pg_connection
from asyncpg import _testbase as tb


MAX_RUNTIME = 0.5


class TestTimeout(tb.ConnectedTestCase):

    async def test_timeout_01(self):
        for methname in {'fetch', 'fetchrow', 'fetchval', 'execute'}:
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(MAX_RUNTIME):
                meth = getattr(self.con, methname)
                await meth('select pg_sleep(10)', timeout=0.02)
            self.assertEqual(await self.con.fetch('select 1'), [(1,)])

    async def test_timeout_02(self):
        st = await self.con.prepare('select pg_sleep(10)')

        for methname in {'fetch', 'fetchrow', 'fetchval'}:
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(MAX_RUNTIME):
                meth = getattr(st, methname)
                await meth(timeout=0.02)
            self.assertEqual(await self.con.fetch('select 1'), [(1,)])

    async def test_timeout_03(self):
        task = self.loop.create_task(
            self.con.fetch('select pg_sleep(10)', timeout=0.2))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError), \
                self.assertRunUnder(MAX_RUNTIME):
            await task
        self.assertEqual(await self.con.fetch('select 1'), [(1,)])

    async def test_timeout_04(self):
        st = await self.con.prepare('select pg_sleep(10)', timeout=0.1)
        with self.assertRaises(asyncio.TimeoutError), \
                self.assertRunUnder(MAX_RUNTIME):
            async with self.con.transaction():
                async for _ in st.cursor(timeout=0.1):  # NOQA
                    pass
        self.assertEqual(await self.con.fetch('select 1'), [(1,)])

        st = await self.con.prepare('select pg_sleep(10)', timeout=0.1)
        async with self.con.transaction():
            cur = await st.cursor()
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(MAX_RUNTIME):
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
                    self.assertRunUnder(MAX_RUNTIME):
                async for _ in self.con.cursor(   # NOQA
                        'select pg_sleep(10)', timeout=0.1):
                    pass
        self.assertEqual(await self.con.fetch('select 1'), [(1,)])

        async with self.con.transaction():
            cur = await self.con.cursor('select pg_sleep(10)')
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(MAX_RUNTIME):
                await cur.fetch(1, timeout=0.1)

        async with self.con.transaction():
            cur = await self.con.cursor('select pg_sleep(10)')
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(MAX_RUNTIME):
                await cur.forward(1, timeout=1e-10)

        async with self.con.transaction():
            cur = await self.con.cursor('select pg_sleep(10)')
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(MAX_RUNTIME):
                await cur.fetchrow(timeout=0.1)

        async with self.con.transaction():
            cur = await self.con.cursor('select pg_sleep(10)')
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(MAX_RUNTIME):
                await cur.fetchrow(timeout=0.1)

            with self.assertRaises(asyncpg.InFailedSQLTransactionError):
                await cur.fetch(1)

        self.assertEqual(await self.con.fetch('select 1'), [(1,)])

    async def test_invalid_timeout(self):
        for command_timeout in ('a', False, -1):
            with self.subTest(command_timeout=command_timeout):
                with self.assertRaisesRegex(ValueError,
                                            'invalid command_timeout'):
                    await self.connect(command_timeout=command_timeout)

        # Note: negative timeouts are OK for method calls.
        for methname in {'fetch', 'fetchrow', 'fetchval', 'execute'}:
            for timeout in ('a', False):
                with self.subTest(timeout=timeout):
                    with self.assertRaisesRegex(ValueError, 'invalid timeout'):
                        await self.con.execute('SELECT 1', timeout=timeout)


class TestConnectionCommandTimeout(tb.ConnectedTestCase):

    @tb.with_connection_options(command_timeout=0.2)
    async def test_command_timeout_01(self):
        for methname in {'fetch', 'fetchrow', 'fetchval', 'execute'}:
            with self.assertRaises(asyncio.TimeoutError), \
                    self.assertRunUnder(MAX_RUNTIME):
                meth = getattr(self.con, methname)
                await meth('select pg_sleep(10)')
            self.assertEqual(await self.con.fetch('select 1'), [(1,)])


class SlowPrepareConnection(pg_connection.Connection):
    """Connection class to test timeouts."""
    async def _get_statement(self, query, timeout, **kwargs):
        await asyncio.sleep(0.3)
        return await super()._get_statement(query, timeout, **kwargs)


class TestTimeoutCoversPrepare(tb.ConnectedTestCase):

    @tb.with_connection_options(connection_class=SlowPrepareConnection,
                                command_timeout=0.3)
    async def test_timeout_covers_prepare_01(self):
        for methname in {'fetch', 'fetchrow', 'fetchval', 'execute'}:
            with self.assertRaises(asyncio.TimeoutError):
                meth = getattr(self.con, methname)
                await meth('select pg_sleep($1)', 0.2)
