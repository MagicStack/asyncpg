# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncpg

from asyncpg import _testbase as tb


class TestTransaction(tb.ConnectedTestCase):

    async def test_transaction_regular(self):
        self.assertIsNone(self.con._top_xact)
        tr = self.con.transaction()
        self.assertIsNone(self.con._top_xact)

        with self.assertRaises(ZeroDivisionError):
            async with tr as with_tr:
                self.assertIs(self.con._top_xact, tr)

                # We don't return the transaction object from __aenter__,
                # to make it harder for people to use '.rollback()' and
                # '.commit()' from within an 'async with' block.
                self.assertIsNone(with_tr)

                await self.con.execute('''
                    CREATE TABLE mytab (a int);
                ''')

                1 / 0

        self.assertIsNone(self.con._top_xact)

        with self.assertRaisesRegex(asyncpg.PostgresError,
                                    '"mytab" does not exist'):
            await self.con.prepare('''
                SELECT * FROM mytab
            ''')

    async def test_transaction_nested(self):
        self.assertIsNone(self.con._top_xact)
        tr = self.con.transaction()
        self.assertIsNone(self.con._top_xact)

        with self.assertRaises(ZeroDivisionError):
            async with tr:
                self.assertIs(self.con._top_xact, tr)

                await self.con.execute('''
                    CREATE TABLE mytab (a int);
                ''')

                async with self.con.transaction():
                    self.assertIs(self.con._top_xact, tr)

                    await self.con.execute('''
                        INSERT INTO mytab (a) VALUES (1), (2);
                    ''')

                self.assertIs(self.con._top_xact, tr)

                with self.assertRaises(ZeroDivisionError):
                    in_tr = self.con.transaction()
                    async with in_tr:

                        self.assertIs(self.con._top_xact, tr)

                        await self.con.execute('''
                            INSERT INTO mytab (a) VALUES (3), (4);
                        ''')

                        1 / 0

                st = await self.con.prepare('SELECT * FROM mytab;')

                recs = []
                async for rec in st.cursor():
                    recs.append(rec)

                self.assertEqual(len(recs), 2)
                self.assertEqual(recs[0][0], 1)
                self.assertEqual(recs[1][0], 2)

                self.assertIs(self.con._top_xact, tr)

                1 / 0

        self.assertIs(self.con._top_xact, None)

        with self.assertRaisesRegex(asyncpg.PostgresError,
                                    '"mytab" does not exist'):
            await self.con.prepare('''
                SELECT * FROM mytab
            ''')

    async def test_transaction_interface_errors(self):
        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction(readonly=True, isolation='serializable')
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot start; .* already started'):
            async with tr:
                await tr.start()

        self.assertTrue(repr(tr).startswith(
            '<asyncpg.Transaction state:rolledback serializable readonly'))

        self.assertIsNone(self.con._top_xact)

        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot start; .* already rolled back'):
            async with tr:
                pass

        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot manually commit.*async with'):
            async with tr:
                await tr.commit()

        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot manually rollback.*async with'):
            async with tr:
                await tr.rollback()

        self.assertIsNone(self.con._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot enter context:.*async with'):
            async with tr:
                async with tr:
                    pass
