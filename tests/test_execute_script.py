from asyncpg import _testbase as tb


class TestExecuteScript(tb.ConnectedTestCase):

    async def test_execute_script_1(self):
        r = await self.con.execute_script('''
            SELECT 1;

            SELECT true FROM pg_type WHERE false = true;

            SELECT 2;
        ''')
        self.assertIsNone(r)
