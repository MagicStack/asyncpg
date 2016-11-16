# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import asyncpg

from asyncpg import _testbase as tb


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

    async def test_execute_many_1(self):
        await self.con.execute('CREATE TEMP TABLE exmany (a text, b int)')

        try:
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
            result = await self.con.executemany('''
                INSERT INTO exmany VALUES($1, $2)
            ''', ())

            result = await self.con.fetch('''
                SELECT * FROM exmany
            ''')

            self.assertEqual(result, [
                ('a', 1), ('b', 2), ('c', 3), ('d', 4)
            ])
        finally:
            await self.con.execute('DROP TABLE exmany')
