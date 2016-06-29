import asyncpg

from asyncpg import _testbase as tb


class TestTransaction(tb.ConnectedTestCase):

    async def test_transaction_regular(self):

        try:
            async with self.con.transaction():

                await self.con.execute_script('''
                    CREATE TABLE mytab (a int);
                ''')

                1 / 0

        except ZeroDivisionError:
            pass
        else:
            self.fail('ZeroDivisionError was not raised')

        with self.assertRaisesRegex(asyncpg.Error, '"mytab" does not exist'):
            await self.con.prepare('''
                SELECT * FROM mytab
            ''')

    async def test_transaction_nested(self):

        try:
            async with self.con.transaction():

                await self.con.execute_script('''
                    CREATE TABLE mytab (a int);
                ''')

                async with self.con.transaction():

                    await self.con.execute_script('''
                        INSERT INTO mytab (a) VALUES (1), (2);
                    ''')

                try:
                    async with self.con.transaction():

                        await self.con.execute_script('''
                            INSERT INTO mytab (a) VALUES (3), (4);
                        ''')

                        1 / 0
                except ZeroDivisionError:
                    pass
                else:
                    self.fail('ZeroDivisionError was not raised')

                res = await self.con.execute('SELECT * FROM mytab;')
                self.assertEqual(len(res), 2)
                self.assertEqual(res[0][0], 1)
                self.assertEqual(res[1][0], 2)

                1 / 0

        except ZeroDivisionError:
            pass
        else:
            self.fail('ZeroDivisionError was not raised')

        with self.assertRaisesRegex(asyncpg.Error, '"mytab" does not exist'):
            await self.con.prepare('''
                SELECT * FROM mytab
            ''')
