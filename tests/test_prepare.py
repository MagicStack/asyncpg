from asyncpg import _testbase as tb


class TestPrepare(tb.ConnectedTestCase):

    async def test_prepare_1(self):
        st = await self.con.prepare('SELECT 1 = $1')
        self.assertEqual([[True]], await st.execute(1))
        self.assertEqual([[False]], await st.execute(10))

    async def test_prepare_2(self):
        with self.assertRaisesRegex(Exception, 'column "a" does not exist'):
            await self.con.prepare('SELECT a')
