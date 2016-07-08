import asyncpg

from asyncpg import _testbase as tb


class TestTransaction(tb.ConnectedTestCase):

    async def test_transaction_regular(self):

        try:
            async with self.con.transaction():

                await self.con.execute('''
                    CREATE TABLE mytab (a int);
                ''')

                1 / 0

        except ZeroDivisionError:
            pass
        else:
            self.fail('ZeroDivisionError was not raised')

        with self.assertRaisesRegex(asyncpg.PostgresError,
                                    '"mytab" does not exist'):
            await self.con.prepare('''
                SELECT * FROM mytab
            ''')

    async def test_transaction_nested(self):

        try:
            async with self.con.transaction():

                await self.con.execute('''
                    CREATE TABLE mytab (a int);
                ''')

                async with self.con.transaction():

                    await self.con.execute('''
                        INSERT INTO mytab (a) VALUES (1), (2);
                    ''')

                try:
                    async with self.con.transaction():

                        await self.con.execute('''
                            INSERT INTO mytab (a) VALUES (3), (4);
                        ''')

                        1 / 0
                except ZeroDivisionError:
                    pass
                else:
                    self.fail('ZeroDivisionError was not raised')

                st = await self.con.prepare('SELECT * FROM mytab;')

                recs = []
                async for rec in st.get_aiter():
                    recs.append(rec)

                self.assertEqual(len(recs), 2)
                self.assertEqual(recs[0][0], 1)
                self.assertEqual(recs[1][0], 2)

                1 / 0

        except ZeroDivisionError:
            pass
        else:
            self.fail('ZeroDivisionError was not raised')

        with self.assertRaisesRegex(asyncpg.PostgresError, '"mytab" does not exist'):
            await self.con.prepare('''
                SELECT * FROM mytab
            ''')
