import asyncio

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
