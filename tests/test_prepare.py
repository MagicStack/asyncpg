from asyncpg import _testbase as tb


class TestPrepare(tb.ConnectedTestCase):

    async def test_prepare_1(self):
        st = await self.con.prepare('SELECT 1 = $1 AS test')

        rec = (await st.execute(1))[0]
        self.assertTrue(rec['test'])
        self.assertEqual(len(rec), 1)
        self.assertEqual(tuple(rec), (True,))

        self.assertEqual(False, (await st.execute(10))[0][0])

    async def test_prepare_2(self):
        with self.assertRaisesRegex(Exception, 'column "a" does not exist'):
            await self.con.prepare('SELECT a')

    async def test_prepare_3(self):
        st = await self.con.prepare('''
            SELECT CASE WHEN $1::text IS NULL THEN 'NULL'
                                              ELSE $1::text END''')

        self.assertEqual((await st.execute('aaa'))[0][0], 'aaa')
        self.assertEqual((await st.execute(None))[0][0], 'NULL')
