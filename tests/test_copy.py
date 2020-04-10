# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import datetime
import io
import os
import tempfile

import asyncpg
from asyncpg import _testbase as tb
from asyncpg import compat


class TestCopyFrom(tb.ConnectedTestCase):

    async def test_copy_from_table_basics(self):
        await self.con.execute('''
            CREATE TABLE copytab(a text, "b~" text, i int);
            INSERT INTO copytab (a, "b~", i) (
                SELECT 'a' || i::text, 'b' || i::text, i
                FROM generate_series(1, 5) AS i
            );
            INSERT INTO copytab (a, "b~", i) VALUES('*', NULL, NULL);
        ''')

        try:
            f = io.BytesIO()

            # Basic functionality.
            res = await self.con.copy_from_table('copytab', output=f)

            self.assertEqual(res, 'COPY 6')

            output = f.getvalue().decode().split('\n')
            self.assertEqual(
                output,
                [
                    'a1\tb1\t1',
                    'a2\tb2\t2',
                    'a3\tb3\t3',
                    'a4\tb4\t4',
                    'a5\tb5\t5',
                    '*\t\\N\t\\N',
                    ''
                ]
            )

            # Test parameters.
            await self.con.execute('SET search_path=none')

            f.seek(0)
            f.truncate()

            res = await self.con.copy_from_table(
                'copytab', output=f, columns=('a', 'b~'),
                schema_name='public', format='csv',
                delimiter='|', null='n-u-l-l', header=True,
                quote='*', escape='!', force_quote=('a',))

            output = f.getvalue().decode().split('\n')

            self.assertEqual(
                output,
                [
                    'a|b~',
                    '*a1*|b1',
                    '*a2*|b2',
                    '*a3*|b3',
                    '*a4*|b4',
                    '*a5*|b5',
                    '*!**|n-u-l-l',
                    ''
                ]
            )

            await self.con.execute('SET search_path=public')
        finally:
            await self.con.execute('DROP TABLE public.copytab')

    async def test_copy_from_table_large_rows(self):
        await self.con.execute('''
            CREATE TABLE copytab(a text, b text);
            INSERT INTO copytab (a, b) (
                SELECT
                    repeat('a' || i::text, 500000),
                    repeat('b' || i::text, 500000)
                FROM
                    generate_series(1, 5) AS i
            );
        ''')

        try:
            f = io.BytesIO()

            # Basic functionality.
            res = await self.con.copy_from_table('copytab', output=f)

            self.assertEqual(res, 'COPY 5')

            output = f.getvalue().decode().split('\n')
            self.assertEqual(
                output,
                [
                    'a1' * 500000 + '\t' + 'b1' * 500000,
                    'a2' * 500000 + '\t' + 'b2' * 500000,
                    'a3' * 500000 + '\t' + 'b3' * 500000,
                    'a4' * 500000 + '\t' + 'b4' * 500000,
                    'a5' * 500000 + '\t' + 'b5' * 500000,
                    ''
                ]
            )
        finally:
            await self.con.execute('DROP TABLE public.copytab')

    async def test_copy_from_query_basics(self):
        f = io.BytesIO()

        res = await self.con.copy_from_query('''
            SELECT
                repeat('a' || i::text, 500000),
                repeat('b' || i::text, 500000)
            FROM
                generate_series(1, 5) AS i
        ''', output=f)

        self.assertEqual(res, 'COPY 5')

        output = f.getvalue().decode().split('\n')
        self.assertEqual(
            output,
            [
                'a1' * 500000 + '\t' + 'b1' * 500000,
                'a2' * 500000 + '\t' + 'b2' * 500000,
                'a3' * 500000 + '\t' + 'b3' * 500000,
                'a4' * 500000 + '\t' + 'b4' * 500000,
                'a5' * 500000 + '\t' + 'b5' * 500000,
                ''
            ]
        )

    async def test_copy_from_query_with_args(self):
        f = io.BytesIO()

        res = await self.con.copy_from_query('''
            SELECT
                i, i * 10
            FROM
                generate_series(1, 5) AS i
            WHERE
                i = $1
        ''', 3, output=f)

        self.assertEqual(res, 'COPY 1')

        output = f.getvalue().decode().split('\n')
        self.assertEqual(
            output,
            [
                '3\t30',
                ''
            ]
        )

    async def test_copy_from_query_to_path(self):
        with tempfile.NamedTemporaryFile() as f:
            f.close()
            await self.con.copy_from_query('''
                SELECT
                    i, i * 10
                FROM
                    generate_series(1, 5) AS i
                WHERE
                    i = $1
            ''', 3, output=f.name)

            with open(f.name, 'rb') as fr:
                output = fr.read().decode().split('\n')
                self.assertEqual(
                    output,
                    [
                        '3\t30',
                        ''
                    ]
                )

    async def test_copy_from_query_to_path_like(self):
        with tempfile.NamedTemporaryFile() as f:
            f.close()

            class Path:
                def __init__(self, path):
                    self.path = path

                def __fspath__(self):
                    return self.path

            await self.con.copy_from_query('''
                SELECT
                    i, i * 10
                FROM
                    generate_series(1, 5) AS i
                WHERE
                    i = $1
            ''', 3, output=Path(f.name))

            with open(f.name, 'rb') as fr:
                output = fr.read().decode().split('\n')
                self.assertEqual(
                    output,
                    [
                        '3\t30',
                        ''
                    ]
                )

    async def test_copy_from_query_to_bad_output(self):
        with self.assertRaisesRegex(TypeError, 'output is expected to be'):
            await self.con.copy_from_query('''
                SELECT
                    i, i * 10
                FROM
                    generate_series(1, 5) AS i
                WHERE
                    i = $1
            ''', 3, output=1)

    async def test_copy_from_query_to_sink(self):
        with tempfile.NamedTemporaryFile() as f:
            async def writer(data):
                # Sleeping here to simulate slow output sink to test
                # backpressure.
                await asyncio.sleep(0.05)
                f.write(data)

            await self.con.copy_from_query('''
                SELECT
                    repeat('a', 500)
                FROM
                    generate_series(1, 5000) AS i
            ''', output=writer)

            f.seek(0)

            output = f.read().decode().split('\n')
            self.assertEqual(
                output,
                [
                    'a' * 500
                ] * 5000 + ['']
            )

        self.assertEqual(await self.con.fetchval('SELECT 1'), 1)

    async def test_copy_from_query_cancellation_explicit(self):
        async def writer(data):
            # Sleeping here to simulate slow output sink to test
            # backpressure.
            await asyncio.sleep(0.5)

        coro = self.con.copy_from_query('''
            SELECT
                repeat('a', 500)
            FROM
                generate_series(1, 5000) AS i
        ''', output=writer)

        task = self.loop.create_task(coro)
        await asyncio.sleep(0.7)
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(await self.con.fetchval('SELECT 1'), 1)

    async def test_copy_from_query_cancellation_on_sink_error(self):
        async def writer(data):
            await asyncio.sleep(0.05)
            raise RuntimeError('failure')

        coro = self.con.copy_from_query('''
            SELECT
                repeat('a', 500)
            FROM
                generate_series(1, 5000) AS i
        ''', output=writer)

        task = self.loop.create_task(coro)

        with self.assertRaises(RuntimeError):
            await task

        self.assertEqual(await self.con.fetchval('SELECT 1'), 1)

    async def test_copy_from_query_cancellation_while_waiting_for_data(self):
        async def writer(data):
            pass

        coro = self.con.copy_from_query('''
            SELECT
                pg_sleep(60)
            FROM
                generate_series(1, 5000) AS i
        ''', output=writer)

        task = self.loop.create_task(coro)
        await asyncio.sleep(0.7)
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(await self.con.fetchval('SELECT 1'), 1)

    async def test_copy_from_query_timeout_1(self):
        async def writer(data):
            await asyncio.sleep(0.05)

        coro = self.con.copy_from_query('''
            SELECT
                repeat('a', 500)
            FROM
                generate_series(1, 5000) AS i
        ''', output=writer, timeout=0.10)

        task = self.loop.create_task(coro)

        with self.assertRaises(asyncio.TimeoutError):
            await task

        self.assertEqual(await self.con.fetchval('SELECT 1'), 1)

    async def test_copy_from_query_timeout_2(self):
        async def writer(data):
            try:
                await asyncio.sleep(10)
            except asyncio.TimeoutError:
                raise
            else:
                self.fail('TimeoutError not raised')

        coro = self.con.copy_from_query('''
            SELECT
                repeat('a', 500)
            FROM
                generate_series(1, 5000) AS i
        ''', output=writer, timeout=0.10)

        task = self.loop.create_task(coro)

        with self.assertRaises(asyncio.TimeoutError):
            await task

        self.assertEqual(await self.con.fetchval('SELECT 1'), 1)


class TestCopyTo(tb.ConnectedTestCase):

    async def test_copy_to_table_basics(self):
        await self.con.execute('''
            CREATE TABLE copytab(a text, "b~" text, i int);
        ''')

        try:
            f = io.BytesIO()
            f.write(
                '\n'.join([
                    'a1\tb1\t1',
                    'a2\tb2\t2',
                    'a3\tb3\t3',
                    'a4\tb4\t4',
                    'a5\tb5\t5',
                    '*\t\\N\t\\N',
                    ''
                ]).encode('utf-8')
            )
            f.seek(0)

            res = await self.con.copy_to_table('copytab', source=f)
            self.assertEqual(res, 'COPY 6')

            output = await self.con.fetch("""
                SELECT * FROM copytab ORDER BY a
            """)
            self.assertEqual(
                output,
                [
                    ('*', None, None),
                    ('a1', 'b1', 1),
                    ('a2', 'b2', 2),
                    ('a3', 'b3', 3),
                    ('a4', 'b4', 4),
                    ('a5', 'b5', 5),
                ]
            )

            # Test parameters.
            await self.con.execute('TRUNCATE copytab')
            await self.con.execute('SET search_path=none')

            f.seek(0)
            f.truncate()

            f.write(
                '\n'.join([
                    'a|b~',
                    '*a1*|b1',
                    '*a2*|b2',
                    '*a3*|b3',
                    '*a4*|b4',
                    '*a5*|b5',
                    '*!**|*n-u-l-l*',
                    'n-u-l-l|bb'
                ]).encode('utf-8')
            )
            f.seek(0)

            if self.con.get_server_version() < (9, 4):
                force_null = None
                forced_null_expected = 'n-u-l-l'
            else:
                force_null = ('b~',)
                forced_null_expected = None

            res = await self.con.copy_to_table(
                'copytab', source=f, columns=('a', 'b~'),
                schema_name='public', format='csv',
                delimiter='|', null='n-u-l-l', header=True,
                quote='*', escape='!', force_not_null=('a',),
                force_null=force_null)

            self.assertEqual(res, 'COPY 7')

            await self.con.execute('SET search_path=public')

            output = await self.con.fetch("""
                SELECT * FROM copytab ORDER BY a
            """)
            self.assertEqual(
                output,
                [
                    ('*', forced_null_expected, None),
                    ('a1', 'b1', None),
                    ('a2', 'b2', None),
                    ('a3', 'b3', None),
                    ('a4', 'b4', None),
                    ('a5', 'b5', None),
                    ('n-u-l-l', 'bb', None),
                ]
            )

        finally:
            await self.con.execute('DROP TABLE public.copytab')

    async def test_copy_to_table_large_rows(self):
        await self.con.execute('''
            CREATE TABLE copytab(a text, b text);
        ''')

        try:
            class _Source:
                def __init__(self):
                    self.rowcount = 0

                @compat.aiter_compat
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    if self.rowcount >= 100:
                        raise StopAsyncIteration
                    else:
                        self.rowcount += 1
                        return b'a1' * 500000 + b'\t' + b'b1' * 500000 + b'\n'

            res = await self.con.copy_to_table('copytab', source=_Source())

            self.assertEqual(res, 'COPY 100')

        finally:
            await self.con.execute('DROP TABLE copytab')

    async def test_copy_to_table_from_bytes_like(self):
        await self.con.execute('''
            CREATE TABLE copytab(a text, b text);
        ''')

        try:
            data = memoryview((b'a1' * 500 + b'\t' + b'b1' * 500 + b'\n') * 2)
            res = await self.con.copy_to_table('copytab', source=data)
            self.assertEqual(res, 'COPY 2')
        finally:
            await self.con.execute('DROP TABLE copytab')

    async def test_copy_to_table_fail_in_source_1(self):
        await self.con.execute('''
            CREATE TABLE copytab(a text, b text);
        ''')

        try:
            class _Source:
                def __init__(self):
                    self.rowcount = 0

                @compat.aiter_compat
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    raise RuntimeError('failure in source')

            with self.assertRaisesRegex(RuntimeError, 'failure in source'):
                await self.con.copy_to_table('copytab', source=_Source())

            # Check that the protocol has recovered.
            self.assertEqual(await self.con.fetchval('SELECT 1'), 1)

        finally:
            await self.con.execute('DROP TABLE copytab')

    async def test_copy_to_table_fail_in_source_2(self):
        await self.con.execute('''
            CREATE TABLE copytab(a text, b text);
        ''')

        try:
            class _Source:
                def __init__(self):
                    self.rowcount = 0

                @compat.aiter_compat
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    if self.rowcount == 0:
                        self.rowcount += 1
                        return b'a\tb\n'
                    else:
                        raise RuntimeError('failure in source')

            with self.assertRaisesRegex(RuntimeError, 'failure in source'):
                await self.con.copy_to_table('copytab', source=_Source())

            # Check that the protocol has recovered.
            self.assertEqual(await self.con.fetchval('SELECT 1'), 1)

        finally:
            await self.con.execute('DROP TABLE copytab')

    async def test_copy_to_table_timeout(self):
        await self.con.execute('''
            CREATE TABLE copytab(a text, b text);
        ''')

        try:
            class _Source:
                def __init__(self, loop):
                    self.rowcount = 0
                    self.loop = loop

                @compat.aiter_compat
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    self.rowcount += 1
                    await asyncio.sleep(60)
                    return b'a1' * 50 + b'\t' + b'b1' * 50 + b'\n'

            with self.assertRaises(asyncio.TimeoutError):
                await self.con.copy_to_table(
                    'copytab', source=_Source(self.loop), timeout=0.10)

            # Check that the protocol has recovered.
            self.assertEqual(await self.con.fetchval('SELECT 1'), 1)

        finally:
            await self.con.execute('DROP TABLE copytab')

    async def test_copy_to_table_from_file_path(self):
        await self.con.execute('''
            CREATE TABLE copytab(a text, "b~" text, i int);
        ''')

        f = tempfile.NamedTemporaryFile(delete=False)
        try:
            f.write(
                '\n'.join([
                    'a1\tb1\t1',
                    'a2\tb2\t2',
                    'a3\tb3\t3',
                    'a4\tb4\t4',
                    'a5\tb5\t5',
                    '*\t\\N\t\\N',
                    ''
                ]).encode('utf-8')
            )
            f.close()

            res = await self.con.copy_to_table('copytab', source=f.name)
            self.assertEqual(res, 'COPY 6')

            output = await self.con.fetch("""
                SELECT * FROM copytab ORDER BY a
            """)
            self.assertEqual(
                output,
                [
                    ('*', None, None),
                    ('a1', 'b1', 1),
                    ('a2', 'b2', 2),
                    ('a3', 'b3', 3),
                    ('a4', 'b4', 4),
                    ('a5', 'b5', 5),
                ]
            )

        finally:
            await self.con.execute('DROP TABLE public.copytab')
            os.unlink(f.name)

    async def test_copy_records_to_table_1(self):
        await self.con.execute('''
            CREATE TABLE copytab(a text, b int, c timestamptz);
        ''')

        try:
            date = datetime.datetime.now(tz=datetime.timezone.utc)
            delta = datetime.timedelta(days=1)

            records = [
                ('a-{}'.format(i), i, date + delta)
                for i in range(100)
            ]

            records.append(('a-100', None, None))

            res = await self.con.copy_records_to_table(
                'copytab', records=records)

            self.assertEqual(res, 'COPY 101')

        finally:
            await self.con.execute('DROP TABLE copytab')

    async def test_copy_records_to_table_no_binary_codec(self):
        await self.con.execute('''
            CREATE TABLE copytab(a uuid);
        ''')

        try:
            def _encoder(value):
                return value

            def _decoder(value):
                return value

            await self.con.set_type_codec(
                'uuid', encoder=_encoder, decoder=_decoder,
                schema='pg_catalog', format='text'
            )

            records = [('2975ab9a-f79c-4ab4-9be5-7bc134d952f0',)]

            with self.assertRaisesRegex(
                    asyncpg.InternalClientError, 'no binary format encoder'):
                await self.con.copy_records_to_table(
                    'copytab', records=records)

        finally:
            await self.con.reset_type_codec(
                'uuid', schema='pg_catalog'
            )
            await self.con.execute('DROP TABLE copytab')
