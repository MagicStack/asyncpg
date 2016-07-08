import inspect

from asyncpg import _testbase as tb


class TestPrepare(tb.ConnectedTestCase):

    async def test_prepare_1(self):
        st = await self.con.prepare('SELECT 1 = $1 AS test')

        rec = await st.get_first_row(1)
        self.assertTrue(rec['test'])
        self.assertEqual(len(rec), 1)
        self.assertEqual(tuple(rec), (True,))

        self.assertEqual(False, await st.get_value(10))

    async def test_prepare_2(self):
        with self.assertRaisesRegex(Exception, 'column "a" does not exist'):
            await self.con.prepare('SELECT a')

    async def test_prepare_3(self):
        cases = [
            ('text', ("'NULL'", 'NULL'), [
                'aaa',
                None
            ]),

            ('decimal', ('0', 0), [
                123,
                123.5,
                None
            ])
        ]

        for type, (none_name, none_val), vals in cases:
            st = await self.con.prepare('''
                    SELECT CASE WHEN $1::{type} IS NULL THEN {default}
                    ELSE $1::{type} END'''.format(
                type=type, default=none_name))

            for val in vals:
                with self.subTest(type=type, value=val):
                    res = await st.get_value(val)
                    if val is None:
                        self.assertEqual(res, none_val)
                    else:
                        self.assertEqual(res, val)

    async def test_prepare_4(self):
        s = await self.con.prepare('SELECT $1::smallint')
        self.assertEqual(await s.get_value(10), 10)

        s = await self.con.prepare('SELECT $1::smallint * 2')
        self.assertEqual(await s.get_value(10), 20)

    async def test_prepare_5_unknownoid(self):
        s = await self.con.prepare("SELECT 'test'")
        self.assertEqual(await s.get_value(), 'test')

    async def test_prepare_6_with(self):
        async with self.con.prepare('SELECT $1::smallint') as stmt:
            self.assertEqual(await stmt.get_value(10), 10)

    async def test_prepare_7_with(self):
        with self.assertRaisesRegex(RuntimeError, 'nested.*async with'):
            async with self.con.prepare('SELECT $1::smallint') as stmt:
                async with stmt:
                    pass

        with self.assertRaisesRegex(RuntimeError, 'nested.*async with'):
            s = await self.con.prepare("SELECT 'test'")
            async with s:
                async with s:
                    pass

    async def test_prepare_8_uninitialized(self):
        methods = {'get_parameters', 'get_attributes', 'get_aiter',
                   'get_list', 'get_value', 'get_first_row'}

        stmt = self.con.prepare('SELECT $1::smallint')

        for meth in methods:
            with self.subTest(method=meth, closed=False, initialized=False):
                with self.assertRaisesRegex(RuntimeError, 'not initialized'):
                    val = getattr(stmt, meth)()
                    if inspect.isawaitable(val):
                        await val

        await stmt.free()

        for meth in methods:
            with self.subTest(method=meth, closed=True, initialized=False):
                with self.assertRaisesRegex(RuntimeError, 'cannot.*closed'):
                    val = getattr(stmt, meth)()
                    if inspect.isawaitable(val):
                        await val
