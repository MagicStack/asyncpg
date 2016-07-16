# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncpg
import inspect

from asyncpg import _testbase as tb


class TestIterableCursor(tb.ConnectedTestCase):

    async def test_cursor_iterable_01(self):
        st = await self.con.prepare('SELECT generate_series(0, 20)')
        expected = await st.fetch()

        for prefetch in range(1, 25):
            with self.subTest(prefetch=prefetch):
                async with self.con.transaction():
                    result = []
                    async for rec in st.cursor(prefetch=prefetch):
                        result.append(rec)

                self.assertEqual(
                    result, expected,
                    'result != expected for prefetch={}'.format(prefetch))

    async def test_cursor_iterable_02(self):
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
                                    'cursor cannot be created.*transaction'):
            await it.__anext__()

    async def test_cursor_iterable_03(self):
        st = await self.con.prepare('SELECT generate_series(0, 20)')

        it = st.cursor().__aiter__()
        if inspect.isawaitable(it):
            it = await it

        st._state.mark_closed()

        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'statement is closed'):
            async for _ in it:  # NOQA
                pass

    async def test_cursor_iterable_04(self):
        st = await self.con.prepare('SELECT generate_series(0, 20)')
        st._state.mark_closed()

        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'statement is closed'):
            async for _ in st.cursor():  # NOQA
                pass

    async def test_cursor_iterable_05(self):
        st = await self.con.prepare('SELECT generate_series(0, 20)')
        for prefetch in range(-1, 1):
            with self.subTest(prefetch=prefetch):
                with self.assertRaisesRegex(asyncpg.InterfaceError,
                                            'must be greater than zero'):
                    async for _ in st.cursor(prefetch=prefetch):  # NOQA
                        pass

    async def test_cursor_iterable_06(self):
        recs = []

        async with self.con.transaction():
            async for rec in self.con.cursor(
                    'SELECT generate_series(0, $1::int)', 10):
                recs.append(rec)

        self.assertEqual(recs, [(i,) for i in range(11)])


class TestCursor(tb.ConnectedTestCase):

    async def test_cursor_01(self):
        st = await self.con.prepare('SELECT generate_series(0, 20)')
        with self.assertRaisesRegex(asyncpg.NoActiveSQLTransactionError,
                                    'cursor cannot be created.*transaction'):
            await st.cursor()

    async def test_cursor_02(self):
        st = await self.con.prepare('SELECT generate_series(0, 20)')
        async with self.con.transaction():
            cur = await st.cursor()

            for i in range(-1, 1):
                with self.assertRaisesRegex(asyncpg.InterfaceError,
                                            'greater than zero'):
                    await cur.fetch(i)

            res = await cur.fetch(2)
            self.assertEqual(res, [(0,), (1,)])

            rec = await cur.fetchrow()
            self.assertEqual(rec, (2,))

            r = repr(cur)
            self.assertTrue(r.startswith('<asyncpg.Cursor '))
            self.assertNotIn(' exhausted ', r)
            self.assertIn('"SELECT generate', r)

            moved = await cur.forward(5)
            self.assertEqual(moved, 5)

            rec = await cur.fetchrow()
            self.assertEqual(rec, (8,))

            res = await cur.fetch(100)
            self.assertEqual(res, [(i,) for i in range(9, 21)])

            self.assertIsNone(await cur.fetchrow())
            self.assertEqual(await cur.fetch(5), [])

            r = repr(cur)
            self.assertTrue(r.startswith('<asyncpg.Cursor '))
            self.assertIn(' exhausted ', r)
            self.assertIn('"SELECT generate', r)

    async def test_cursor_03(self):
        st = await self.con.prepare('SELECT generate_series(0, 20)')
        async with self.con.transaction():
            with self.assertRaisesRegex(asyncpg.InterfaceError,
                                        'prefetch argument can only'):
                await st.cursor(prefetch=10)

    async def test_cursor_04(self):
        async with self.con.transaction():
            st = await self.con.cursor('SELECT generate_series(0, 100)')
            await st.forward(42)
            self.assertEqual(await st.fetchrow(), (42,))
