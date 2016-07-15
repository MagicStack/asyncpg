import asyncio
import asyncpg

from asyncpg import _testbase as tb


class TestExecuteScript(tb.ConnectedTestCase):

    async def test_execute_script_1(self):
        status = await self.con.execute('''
            SELECT 1;

            SELECT true FROM pg_type WHERE false = true;

            SELECT generate_series(0, 9);
        ''')
        self.assertEqual(status, 'SELECT 10')

    async def test_execute_script_check_transactionality(self):
        with self.assertRaises(asyncpg.PostgresError):
            await self.con.execute('''
                CREATE TABLE mytab (a int);
                SELECT * FROM mytab WHERE 1 / 0 = 1;
            ''')

        with self.assertRaisesRegex(asyncpg.PostgresError,
                                    '"mytab" does not exist'):

            await self.con.prepare('''
                SELECT * FROM mytab
            ''')

    async def test_execute_exceptions_1(self):
        with self.assertRaisesRegex(asyncpg.PostgresError,
                                    'relation "__dne__" does not exist'):

            await self.con.execute('select * from __dne__')

    async def test_execute_script_interrupted_close(self):
        fut = self.loop.create_task(
            self.con.execute('''SELECT pg_sleep(10)'''))

        await asyncio.sleep(0.2, loop=self.loop)

        self.assertFalse(self.con.is_closed())
        await self.con.close()
        self.assertTrue(self.con.is_closed())

        with self.assertRaisesRegex(asyncpg.ConnectionDoesNotExistError,
                                    'closed in the middle'):
            await fut

    async def test_execute_script_interrupted_terminate(self):
        fut = self.loop.create_task(
            self.con.execute('''SELECT pg_sleep(10)'''))

        await asyncio.sleep(0.2, loop=self.loop)

        self.assertFalse(self.con.is_closed())
        self.con.terminate()
        self.assertTrue(self.con.is_closed())

        with self.assertRaisesRegex(asyncpg.ConnectionDoesNotExistError,
                                    'closed in the middle'):
            await fut

        self.con.terminate()
