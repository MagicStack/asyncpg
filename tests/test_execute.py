from asyncpg import _testbase as tb


class TestExecute(tb.ConnectedTestCase):

    async def test_execute_1(self):
        r = await self.con.execute('SELECT $1::smallint', 10)
        self.assertEqual(r[0][0], 10)

        r = await self.con.execute('SELECT $1::smallint * 2', 10)
        self.assertEqual(r[0][0], 20)

    async def test_execute_unknownoid(self):
        r = await self.con.execute("SELECT 'test'")
        self.assertEqual(r[0][0], 'test')
