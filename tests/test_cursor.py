import asyncpg
import inspect

from asyncpg import _testbase as tb


class TestCursor(tb.ConnectedTestCase):

    async def test_cursor_01(self):
        st = await self.con.prepare('SELECT generate_series(0, 20)')
        expected = await st.fetch()

        for prefetch in range(25):
            async with self.con.transaction():
                result = []
                async for rec in st.cursor(prefetch=prefetch):
                    result.append(rec)

            self.assertEqual(
                result, expected,
                'result != expected for prefetch={}'.format(prefetch))

    async def test_cursor_02(self):
        # Test that it's not possible to create a cursor without hold
        # outside of a transaction
        s = await self.con.prepare(
            'DECLARE t BINARY CURSOR WITHOUT HOLD FOR SELECT 1')
        with self.assertRaises(asyncpg.NoActiveSQLTransactionError):
            await s.fetch()

        # Now test that statement.cursor() does not let you
        # iterate over it outside of a transaction
        st = await self.con.prepare('SELECT generate_series(0, 20)')

        it = st.cursor(prefetch=5).__aiter__()
        if inspect.isawaitable(it):
            it = await it

        with self.assertRaisesRegex(asyncpg.NoActiveSQLTransactionError,
                                    'cursor cannot be iterated.*transaction'):
            await it.__anext__()

    async def test_cursor_03(self):
        st = await self.con.prepare('SELECT generate_series(0, 20)')

        it = st.cursor().__aiter__()
        if inspect.isawaitable(it):
            it = await it

        st._state.mark_closed()

        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'statement is closed'):
            async for _ in it:  # NOQA
                pass

    async def test_cursor_04(self):
        st = await self.con.prepare('SELECT generate_series(0, 20)')
        st._state.mark_closed()

        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'statement is closed'):
            async for _ in st.cursor():  # NOQA
                pass
