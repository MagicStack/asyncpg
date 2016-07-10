import asyncio
import asyncpg
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

        await stmt.close()

        for meth in methods:
            with self.subTest(method=meth, closed=True, initialized=False):
                with self.assertRaisesRegex(RuntimeError, 'cannot.*closed'):
                    val = getattr(stmt, meth)()
                    if inspect.isawaitable(val):
                        await val

    async def test_prepare_9_interrupted_close(self):
        stmt = await self.con.prepare('''SELECT pg_sleep(10)''')
        fut = self.loop.create_task(stmt.get_list())

        await asyncio.sleep(0.2, loop=self.loop)

        self.assertFalse(self.con.is_closed())
        await self.con.close()
        self.assertTrue(self.con.is_closed())

        with self.assertRaisesRegex(asyncpg.ConnectionDoesNotExistError,
                                    'closed in the middle'):
            await fut

        # Test that it's OK to call close again
        await self.con.close()

    async def test_prepare_10_interrupted_terminate(self):
        stmt = await self.con.prepare('''SELECT pg_sleep(10)''')
        fut = self.loop.create_task(stmt.get_value())

        await asyncio.sleep(0.2, loop=self.loop)

        self.assertFalse(self.con.is_closed())
        self.con.terminate()
        self.assertTrue(self.con.is_closed())

        with self.assertRaisesRegex(asyncpg.ConnectionDoesNotExistError,
                                    'closed in the middle'):
            await fut

        # Test that it's OK to call terminate again
        self.con.terminate()

    async def test_prepare_11_connection_reset(self):
        stmt = await self.con.prepare('SELECT $1::smallint')
        await self.con.reset()

        with self.assertRaisesRegex(RuntimeError, 'cannot.*closed'):
            stmt.get_parameters()

    async def test_prepare_12_big_result(self):
        async with self.con.prepare('select generate_series(0,10000)') as stmt:
            result = await stmt.get_list()

        self.assertEqual(len(result), 10001)
        self.assertEqual(
            [r[0] for r in result],
            list(range(10001)))

    async def test_prepare_13_raise_error(self):
        # Stress test ReadBuffer.read_cstr()
        msg = '0' * 1024 * 100
        query = """
        DO language plpgsql $$
        BEGIN
        RAISE EXCEPTION '{}';
        END
        $$;""".format(msg)

        stmt = await self.con.prepare(query)
        with self.assertRaisesRegex(asyncpg.RaiseError, msg):
            await stmt.get_value()
