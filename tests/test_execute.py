# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import asyncpg

from asyncpg import _testbase as tb
from asyncpg.exceptions import UniqueViolationError


class TestExecuteScript(tb.ConnectedTestCase):

    async def test_execute_script_1(self):
        self.assertEqual(self.con._protocol.queries_count, 0)
        status = await self.con.execute('''
            SELECT 1;

            SELECT true FROM pg_type WHERE false = true;

            SELECT generate_series(0, 9);
        ''')
        self.assertEqual(self.con._protocol.queries_count, 1)
        self.assertEqual(status, 'SELECT 10')

    async def test_execute_script_2(self):
        status = await self.con.execute('''
            CREATE TABLE mytab (a int);
        ''')
        self.assertEqual(status, 'CREATE TABLE')

        try:
            status = await self.con.execute('''
                INSERT INTO mytab (a) VALUES ($1), ($2)
            ''', 10, 20)
            self.assertEqual(status, 'INSERT 0 2')
        finally:
            await self.con.execute('DROP TABLE mytab')

    async def test_execute_script_3(self):
        with self.assertRaisesRegex(asyncpg.PostgresSyntaxError,
                                    'cannot insert multiple commands'):

            await self.con.execute('''
                CREATE TABLE mytab (a int);
                INSERT INTO mytab (a) VALUES ($1), ($2);
            ''', 10, 20)

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

        await asyncio.sleep(0.2)

        self.assertFalse(self.con.is_closed())
        await self.con.close()
        self.assertTrue(self.con.is_closed())

        with self.assertRaises(asyncpg.QueryCanceledError):
            await fut

    async def test_execute_script_interrupted_terminate(self):
        fut = self.loop.create_task(
            self.con.execute('''SELECT pg_sleep(10)'''))

        await asyncio.sleep(0.2)

        self.assertFalse(self.con.is_closed())
        self.con.terminate()
        self.assertTrue(self.con.is_closed())

        with self.assertRaisesRegex(asyncpg.ConnectionDoesNotExistError,
                                    'closed in the middle'):
            await fut

        self.con.terminate()


class TestExecuteMany(tb.ConnectedTestCase):
    def setUp(self):
        super().setUp()
        self.loop.run_until_complete(self.con.execute(
            'CREATE TABLE exmany (a text, b int PRIMARY KEY)'))

    def tearDown(self):
        self.loop.run_until_complete(self.con.execute('DROP TABLE exmany'))
        super().tearDown()

    async def test_executemany_basic(self):
        result = await self.con.executemany('''
            INSERT INTO exmany VALUES($1, $2)
        ''', [
            ('a', 1), ('b', 2), ('c', 3), ('d', 4)
        ])

        self.assertIsNone(result)

        result = await self.con.fetch('''
            SELECT * FROM exmany
        ''')

        self.assertEqual(result, [
            ('a', 1), ('b', 2), ('c', 3), ('d', 4)
        ])

        # Empty set
        await self.con.executemany('''
            INSERT INTO exmany VALUES($1, $2)
        ''', ())

        result = await self.con.fetch('''
            SELECT * FROM exmany
        ''')

        self.assertEqual(result, [
            ('a', 1), ('b', 2), ('c', 3), ('d', 4)
        ])

    async def test_executemany_bad_input(self):
        bad_data = ([1 / 0] for v in range(10))

        with self.assertRaises(ZeroDivisionError):
            async with self.con.transaction():
                await self.con.executemany('''
                    INSERT INTO exmany (b)VALUES($1)
                ''', bad_data)

        good_data = ([v] for v in range(10))
        async with self.con.transaction():
            await self.con.executemany('''
                INSERT INTO exmany (b)VALUES($1)
            ''', good_data)

    async def test_executemany_server_failure(self):
        with self.assertRaises(UniqueViolationError):
            await self.con.executemany('''
                INSERT INTO exmany VALUES($1, $2)
            ''', [
                ('a', 1), ('b', 2), ('c', 2), ('d', 4)
            ])
        result = await self.con.fetch('SELECT * FROM exmany')
        self.assertEqual(result, [])

    async def test_executemany_server_failure_after_writes(self):
        with self.assertRaises(UniqueViolationError):
            await self.con.executemany('''
                INSERT INTO exmany VALUES($1, $2)
            ''', [('a' * 32768, x) for x in range(10)] + [
                ('b', 12), ('c', 12), ('d', 14)
            ])
        result = await self.con.fetch('SELECT b FROM exmany')
        self.assertEqual(result, [])

    async def test_executemany_server_failure_during_writes(self):
        # failure at the beginning, server error detected in the middle
        pos = 0

        def gen():
            nonlocal pos
            while pos < 128:
                pos += 1
                if pos < 3:
                    yield ('a', 0)
                else:
                    yield 'a' * 32768, pos

        with self.assertRaises(UniqueViolationError):
            await self.con.executemany('''
                INSERT INTO exmany VALUES($1, $2)
            ''', gen())
        result = await self.con.fetch('SELECT b FROM exmany')
        self.assertEqual(result, [])
        self.assertLess(pos, 128, 'should stop early')

    async def test_executemany_client_failure_after_writes(self):
        with self.assertRaises(ZeroDivisionError):
            await self.con.executemany('''
                INSERT INTO exmany VALUES($1, $2)
            ''', (('a' * 32768, y + y / y) for y in range(10, -1, -1)))
        result = await self.con.fetch('SELECT b FROM exmany')
        self.assertEqual(result, [])

    async def test_executemany_timeout(self):
        with self.assertRaises(asyncio.TimeoutError):
            await self.con.executemany('''
                INSERT INTO exmany VALUES(pg_sleep(0.1) || $1, $2)
            ''', [('a' * 32768, x) for x in range(128)], timeout=0.5)
        result = await self.con.fetch('SELECT * FROM exmany')
        self.assertEqual(result, [])

    async def test_executemany_timeout_flow_control(self):
        event = asyncio.Event()

        async def locker():
            test_func = getattr(self, self._testMethodName).__func__
            opts = getattr(test_func, '__connect_options__', {})
            conn = await self.connect(**opts)
            try:
                tx = conn.transaction()
                await tx.start()
                await conn.execute("UPDATE exmany SET a = '1' WHERE b = 10")
                event.set()
                await asyncio.sleep(1)
                await tx.rollback()
            finally:
                event.set()
                await conn.close()

        await self.con.executemany('''
            INSERT INTO exmany VALUES(NULL, $1)
        ''', [(x,) for x in range(128)])
        fut = asyncio.ensure_future(locker())
        await event.wait()
        with self.assertRaises(asyncio.TimeoutError):
            await self.con.executemany('''
                UPDATE exmany SET a = $1 WHERE b = $2
            ''', [('a' * 32768, x) for x in range(128)], timeout=0.5)
        await fut
        result = await self.con.fetch(
            'SELECT * FROM exmany WHERE a IS NOT NULL')
        self.assertEqual(result, [])

    async def test_executemany_client_failure_in_transaction(self):
        tx = self.con.transaction()
        await tx.start()
        with self.assertRaises(ZeroDivisionError):
            await self.con.executemany('''
                INSERT INTO exmany VALUES($1, $2)
            ''', (('a' * 32768, y + y / y) for y in range(10, -1, -1)))
        result = await self.con.fetch('SELECT b FROM exmany')
        # only 2 batches executed (2 x 4)
        self.assertEqual(
            [x[0] for x in result], [y + 1 for y in range(10, 2, -1)])
        await tx.rollback()
        result = await self.con.fetch('SELECT b FROM exmany')
        self.assertEqual(result, [])

    async def test_executemany_client_server_failure_conflict(self):
        self.con._transport.set_write_buffer_limits(65536 * 64, 16384 * 64)
        with self.assertRaises(UniqueViolationError):
            await self.con.executemany('''
                INSERT INTO exmany VALUES($1, 0)
            ''', (('a' * 32768,) for y in range(4, -1, -1) if y / y))
        result = await self.con.fetch('SELECT b FROM exmany')
        self.assertEqual(result, [])

    async def test_executemany_prepare(self):
        stmt = await self.con.prepare('''
            INSERT INTO exmany VALUES($1, $2)
        ''')
        result = await stmt.executemany([
            ('a', 1), ('b', 2), ('c', 3), ('d', 4)
        ])
        self.assertIsNone(result)
        result = await self.con.fetch('''
            SELECT * FROM exmany
        ''')
        self.assertEqual(result, [
            ('a', 1), ('b', 2), ('c', 3), ('d', 4)
        ])
        # Empty set
        await stmt.executemany(())
        result = await self.con.fetch('''
            SELECT * FROM exmany
        ''')
        self.assertEqual(result, [
            ('a', 1), ('b', 2), ('c', 3), ('d', 4)
        ])
