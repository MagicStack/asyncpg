# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncpg

from asyncpg import _testbase as tb


class TestTransaction(tb.ConnectedTestCase):

    async def test_transaction_regular(self):
        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())
        tr = self.con.transaction()
        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        with self.assertRaises(ZeroDivisionError):
            async with tr as with_tr:
                self.assertIs(self.con._top_xact, tr)
                self.assertTrue(self.con.is_in_transaction())

                # We don't return the transaction object from __aenter__,
                # to make it harder for people to use '.rollback()' and
                # '.commit()' from within an 'async with' block.
                self.assertIsNone(with_tr)

                await self.con.execute('''
                    CREATE TABLE mytab (a int);
                ''')

                1 / 0

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        with self.assertRaisesRegex(asyncpg.PostgresError,
                                    '"mytab" does not exist'):
            await self.con.prepare('''
                SELECT * FROM mytab
            ''')

    async def test_transaction_nested(self):
        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        tr = self.con.transaction()

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        with self.assertRaises(ZeroDivisionError):
            async with tr:
                self.assertIs(self.con._top_xact, tr)
                self.assertTrue(self.con.is_in_transaction())

                await self.con.execute('''
                    CREATE TABLE mytab (a int);
                ''')

                async with self.con.transaction():
                    self.assertIs(self.con._top_xact, tr)
                    self.assertTrue(self.con.is_in_transaction())

                    await self.con.execute('''
                        INSERT INTO mytab (a) VALUES (1), (2);
                    ''')

                self.assertIs(self.con._top_xact, tr)
                self.assertTrue(self.con.is_in_transaction())

                with self.assertRaises(ZeroDivisionError):
                    in_tr = self.con.transaction()
                    async with in_tr:

                        self.assertIs(self.con._top_xact, tr)
                        self.assertTrue(self.con.is_in_transaction())

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
                self.assertTrue(self.con.is_in_transaction())

                1 / 0

        self.assertIs(self.con._top_xact, None)
        self.assertFalse(self.con.is_in_transaction())

        with self.assertRaisesRegex(asyncpg.PostgresError,
                                    '"mytab" does not exist'):
            await self.con.prepare('''
                SELECT * FROM mytab
            ''')

    async def test_transaction_interface_errors(self):
        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        tr = self.con.transaction(readonly=True, isolation='serializable')
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot start; .* already started'):
            async with tr:
                await tr.start()

        self.assertTrue(repr(tr).startswith(
            '<asyncpg.Transaction state:rolledback serializable readonly'))

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot start; .* already rolled back'):
            async with tr:
                pass

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        tr = self.con.transaction()
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot manually commit.*async with'):
            async with tr:
                await tr.commit()

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        tr = self.con.transaction()
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot manually rollback.*async with'):
            async with tr:
                await tr.rollback()

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        tr = self.con.transaction()
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot enter context:.*async with'):
            async with tr:
                async with tr:
                    pass

    async def test_transaction_within_manual_transaction(self):
        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        await self.con.execute('BEGIN')

        tr = self.con.transaction()
        self.assertIsNone(self.con._top_xact)
        self.assertTrue(self.con.is_in_transaction())

        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot use Connection.transaction'):
            await tr.start()

        with self.assertLoopErrorHandlerCalled(
                'Resetting connection with an active transaction'):
            await self.con.reset()

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

    async def test_isolation_level(self):
        await self.con.reset()
        default_isolation = await self.con.fetchval(
            'SHOW default_transaction_isolation'
        )
        isolation_levels = {
            None: default_isolation,
            'read_committed': 'read committed',
            'repeatable_read': 'repeatable read',
            'serializable': 'serializable',
        }
        set_sql = 'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL '
        get_sql = 'SHOW TRANSACTION ISOLATION LEVEL'
        for tx_level in isolation_levels:
            for conn_level in isolation_levels:
                with self.subTest(conn=conn_level, tx=tx_level):
                    if conn_level:
                        await self.con.execute(
                            set_sql + isolation_levels[conn_level]
                        )
                    level = await self.con.fetchval(get_sql)
                    self.assertEqual(level, isolation_levels[conn_level])
                    async with self.con.transaction(isolation=tx_level):
                        level = await self.con.fetchval(get_sql)
                        self.assertEqual(
                            level,
                            isolation_levels[tx_level or conn_level],
                        )
                    await self.con.reset()

    async def test_nested_isolation_level(self):
        set_sql = 'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL '
        isolation_levels = {
            'read_committed': 'read committed',
            'repeatable_read': 'repeatable read',
            'serializable': 'serializable',
        }
        for inner in [None] + list(isolation_levels):
            for outer, outer_sql_level in isolation_levels.items():
                for implicit in [False, True]:
                    with self.subTest(
                        implicit=implicit, outer=outer, inner=inner,
                    ):
                        if implicit:
                            await self.con.execute(set_sql + outer_sql_level)
                            outer_level = None
                        else:
                            outer_level = outer

                        async with self.con.transaction(isolation=outer_level):
                            if inner and outer != inner:
                                with self.assertRaisesRegex(
                                    asyncpg.InterfaceError,
                                    'current {!r} != outer {!r}'.format(
                                        inner, outer
                                    )
                                ):
                                    async with self.con.transaction(
                                            isolation=inner,
                                    ):
                                        pass
                            else:
                                async with self.con.transaction(
                                        isolation=inner,
                                ):
                                    pass
