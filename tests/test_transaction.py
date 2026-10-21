# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio

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

    async def test_start_failure_does_not_poison_connection(self):
        other = await self.connect()
        slow = None
        try:
            await self.con.execute('''
                DROP TABLE IF EXISTS test_start_failure;
                CREATE TABLE test_start_failure (a int);
            ''')

            slow = self.loop.create_task(
                self.con.fetchval('SELECT pg_sleep(1)'))
            await asyncio.sleep(0.1)

            with self.assertRaisesRegex(
                    asyncpg.InterfaceError,
                    'cannot perform operation: another operation is in '
                    'progress'):
                await self.con.transaction().start()

            # BEGIN never reached the server, so the failed transaction
            # must not stay registered as the top-level transaction.
            self.assertIsNone(self.con._top_xact)

            await slow
            self.assertFalse(self.con.is_in_transaction())

            async with self.con.transaction():
                await self.con.execute(
                    'INSERT INTO test_start_failure VALUES (1)')

            self.assertIsNone(self.con._top_xact)
            self.assertFalse(self.con.is_in_transaction())
            self.assertEqual(
                await other.fetch('SELECT a FROM test_start_failure'),
                [(1,)])
        finally:
            if slow is not None and not slow.done():
                slow.cancel()
                try:
                    await slow
                except asyncio.CancelledError:
                    pass
            if (self.con.is_in_transaction() or
                    self.con._top_xact is not None):
                self.con.terminate()
            try:
                await other.execute(
                    'DROP TABLE IF EXISTS test_start_failure')
            finally:
                await other.close()

    async def test_cancelled_start_does_not_poison_connection(self):
        other = await self.connect()
        try:
            await self.con.execute('''
                DROP TABLE IF EXISTS test_cancelled_start;
                CREATE TABLE test_cancelled_start (a int);
            ''')

            tr = self.con.transaction()

            async def start_transaction():
                async with tr:
                    await self.con.execute(
                        'INSERT INTO test_cancelled_start VALUES (1)')

            task = self.loop.create_task(start_transaction())
            self.loop.call_soon(task.cancel)

            with self.assertRaises(asyncio.CancelledError):
                await task

            tr = self.con.transaction()
            try:
                await tr.start()
            except asyncpg.InterfaceError as ex:
                # The server did execute BEGIN and is still inside the
                # abandoned transaction, so reuse is explicitly refused.
                self.assertEqual(
                    str(ex),
                    'cannot use Connection.transaction() in a manually '
                    'started transaction')
                self.assertTrue(self.con.is_in_transaction())

                # Resetting rolls back the abandoned transaction and is
                # expected to report it.
                with self.assertLoopErrorHandlerCalled(
                        'Resetting connection with an active transaction'):
                    await self.con.reset()

                self.assertFalse(self.con.is_in_transaction())
                self.assertIsNone(self.con._top_xact)

                tr = self.con.transaction()
                await tr.start()

            await self.con.execute(
                'INSERT INTO test_cancelled_start VALUES (1)')
            await tr.commit()

            # Either the transaction above joined the abandoned server
            # transaction and committed it, or it was refused and started
            # after a reset.  In both cases the write must be visible.
            self.assertEqual(
                await other.fetch('SELECT a FROM test_cancelled_start'),
                [(1,)])
            self.assertIsNone(self.con._top_xact)
            self.assertFalse(self.con.is_in_transaction())
        finally:
            if (self.con.is_in_transaction() or
                    self.con._top_xact is not None):
                self.con.terminate()
            try:
                await other.execute(
                    'DROP TABLE IF EXISTS test_cancelled_start')
            finally:
                await other.close()

    async def test_cancelled_start_refuses_reuse_until_reset(self):
        other = await self.connect()
        try:
            await self.con.execute('''
                DROP TABLE IF EXISTS test_cancelled_refused;
                CREATE TABLE test_cancelled_refused (a int);
            ''')

            tr = self.con.transaction()

            async def start_transaction():
                async with tr:
                    pass

            task = self.loop.create_task(start_transaction())
            self.loop.call_soon(task.cancel)

            with self.assertRaises(asyncio.CancelledError):
                await task

            # Wait for the cancellation to be acknowledged.  The server
            # has executed BEGIN, so the connection is inside a
            # transaction that has no Transaction object anymore.
            await self.con.execute('SELECT 1')

            self.assertIsNone(self.con._top_xact)
            self.assertTrue(self.con.is_in_transaction())

            with self.assertRaisesRegex(
                    asyncpg.InterfaceError,
                    r'cannot use Connection.transaction\(\) in a manually '
                    'started transaction'):
                await self.con.transaction().start()

            self.assertIsNone(self.con._top_xact)
            self.assertTrue(self.con.is_in_transaction())

            with self.assertLoopErrorHandlerCalled(
                    'Resetting connection with an active transaction'):
                await self.con.reset()

            self.assertFalse(self.con.is_in_transaction())
            self.assertIsNone(self.con._top_xact)

            async with self.con.transaction():
                await self.con.execute(
                    'INSERT INTO test_cancelled_refused VALUES (1)')

            self.assertEqual(
                await other.fetch('SELECT a FROM test_cancelled_refused'),
                [(1,)])
        finally:
            if (self.con.is_in_transaction() or
                    self.con._top_xact is not None):
                self.con.terminate()
            try:
                await other.execute(
                    'DROP TABLE IF EXISTS test_cancelled_refused')
            finally:
                await other.close()

    async def test_nested_start_failure_keeps_outer_transaction(self):
        other = await self.connect()
        try:
            await self.con.execute('''
                DROP TABLE IF EXISTS test_nested_start;
                CREATE TABLE test_nested_start (a int);
            ''')

            outer = self.con.transaction(isolation='serializable')
            await outer.start()
            await self.con.execute(
                'INSERT INTO test_nested_start VALUES (1)')

            with self.assertRaisesRegex(
                    asyncpg.InterfaceError,
                    'nested transaction has a different isolation level'):
                await self.con.transaction(
                    isolation='read_committed').start()

            self.assertIs(self.con._top_xact, outer)
            self.assertTrue(self.con.is_in_transaction())

            await self.con.execute(
                'INSERT INTO test_nested_start VALUES (2)')
            await outer.commit()

            self.assertIsNone(self.con._top_xact)
            self.assertFalse(self.con.is_in_transaction())

            async with self.con.transaction():
                await self.con.execute(
                    'INSERT INTO test_nested_start VALUES (3)')

            self.assertEqual(
                await other.fetch(
                    'SELECT a FROM test_nested_start ORDER BY a'),
                [(1,), (2,), (3,)])
        finally:
            if (self.con.is_in_transaction() or
                    self.con._top_xact is not None):
                self.con.terminate()
            try:
                await other.execute(
                    'DROP TABLE IF EXISTS test_nested_start')
            finally:
                await other.close()

    async def test_nested_savepoint_failure_keeps_outer_transaction(self):
        other = await self.connect()
        try:
            await self.con.execute('''
                DROP TABLE IF EXISTS test_nested_savepoint;
                CREATE TABLE test_nested_savepoint (a int);
            ''')

            outer = self.con.transaction()
            await outer.start()
            await self.con.execute(
                'INSERT INTO test_nested_savepoint VALUES (1)')

            with self.assertRaises(asyncpg.DivisionByZeroError):
                await self.con.execute('SELECT 1 / 0')

            with self.assertRaises(asyncpg.InFailedSQLTransactionError):
                async with self.con.transaction():
                    await self.con.execute(
                        'INSERT INTO test_nested_savepoint VALUES (2)')

            self.assertIs(self.con._top_xact, outer)
            self.assertTrue(self.con.is_in_transaction())

            await outer.rollback()

            self.assertIsNone(self.con._top_xact)
            self.assertFalse(self.con.is_in_transaction())

            async with self.con.transaction():
                await self.con.execute(
                    'INSERT INTO test_nested_savepoint VALUES (3)')

            self.assertEqual(
                await other.fetch(
                    'SELECT a FROM test_nested_savepoint ORDER BY a'),
                [(3,)])
        finally:
            if (self.con.is_in_transaction() or
                    self.con._top_xact is not None):
                self.con.terminate()
            try:
                await other.execute(
                    'DROP TABLE IF EXISTS test_nested_savepoint')
            finally:
                await other.close()

    async def test_cancelled_start_pool_release(self):
        other = await self.connect()
        try:
            await self.con.execute('''
                DROP TABLE IF EXISTS test_pool_cancelled_start;
                CREATE TABLE test_pool_cancelled_start (a int);
            ''')

            async with self.create_pool(
                    min_size=1, max_size=1) as pool:
                with self.assertLoopErrorHandlerCalled(
                        'Resetting connection with an active transaction'):
                    async with pool.acquire() as con:
                        tr = con.transaction()

                        async def start_transaction():
                            async with tr:
                                await con.execute(
                                    'INSERT INTO '
                                    'test_pool_cancelled_start VALUES (1)')

                        task = self.loop.create_task(start_transaction())
                        self.loop.call_soon(task.cancel)

                        with self.assertRaises(asyncio.CancelledError):
                            await task

                        # Wait for the cancellation to be acknowledged.
                        # The server has executed BEGIN by now and the
                        # connection is inside a transaction it does not
                        # own anymore.
                        await con.execute('SELECT 1')

                        self.assertIsNone(con._con._top_xact)
                        self.assertTrue(con.is_in_transaction())

                async with pool.acquire() as con:
                    self.assertIsNone(con._con._top_xact)
                    self.assertFalse(con.is_in_transaction())
                    async with con.transaction():
                        await con.execute(
                            'INSERT INTO test_pool_cancelled_start '
                            'VALUES (2)')

            self.assertEqual(
                await other.fetch(
                    'SELECT a FROM test_pool_cancelled_start'),
                [(2,)])
        finally:
            try:
                await other.execute(
                    'DROP TABLE IF EXISTS test_pool_cancelled_start')
            finally:
                await other.close()

    async def test_isolation_level(self):
        await self.con.reset()
        default_isolation = await self.con.fetchval(
            'SHOW default_transaction_isolation'
        )
        isolation_levels = {
            None: default_isolation,
            'read_committed': 'read committed',
            'read_uncommitted': 'read uncommitted',
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
            'read_uncommitted': 'read uncommitted',
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
