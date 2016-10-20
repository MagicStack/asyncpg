# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import contextlib
import ipaddress
import os
import platform
import unittest

import asyncpg
from asyncpg import _testbase as tb
from asyncpg.connection import _parse_connect_params

_system = platform.uname().system


class TestSettings(tb.ConnectedTestCase):

    async def test_get_settings_01(self):
        self.assertEqual(
            self.con.get_settings().client_encoding,
            'UTF8')


class TestAuthentication(tb.ConnectedTestCase):
    def setUp(self):
        super().setUp()

        if not self.cluster.is_managed():
            self.skipTest('unmanaged cluster')

        methods = [
            ('trust', None),
            ('reject', None),
            ('md5', 'correctpassword'),
            ('password', 'correctpassword'),
        ]

        self.cluster.reset_hba()

        create_script = []
        for method, password in methods:
            create_script.append(
                'CREATE ROLE {}_user WITH LOGIN{};'.format(
                    method,
                    ' PASSWORD {!r}'.format(password) if password else ''
                )
            )

            if _system != 'Windows':
                self.cluster.add_hba_entry(
                    type='local',
                    database='postgres', user='{}_user'.format(method),
                    auth_method=method)

            self.cluster.add_hba_entry(
                type='host', address=ipaddress.ip_network('127.0.0.0/24'),
                database='postgres', user='{}_user'.format(method),
                auth_method=method)

            self.cluster.add_hba_entry(
                type='host', address=ipaddress.ip_network('::1/128'),
                database='postgres', user='{}_user'.format(method),
                auth_method=method)

        # Put hba changes into effect
        self.cluster.reload()

        create_script = '\n'.join(create_script)
        self.loop.run_until_complete(self.con.execute(create_script))

    def tearDown(self):
        # Reset cluster's pg_hba.conf since we've meddled with it
        self.cluster.trust_local_connections()

        methods = [
            'trust',
            'reject',
            'md5',
            'password',
        ]

        drop_script = []
        for method in methods:
            drop_script.append('DROP ROLE {}_user;'.format(method))

        drop_script = '\n'.join(drop_script)
        self.loop.run_until_complete(self.con.execute(drop_script))

        super().tearDown()

    async def test_auth_bad_user(self):
        with self.assertRaises(
                asyncpg.InvalidAuthorizationSpecificationError):
            await self.cluster.connect(user='__nonexistent__',
                                       database='postgres',
                                       loop=self.loop)

    async def test_auth_trust(self):
        conn = await self.cluster.connect(
            user='trust_user', database='postgres', loop=self.loop)
        await conn.close()

    async def test_auth_reject(self):
        with self.assertRaisesRegex(
                asyncpg.InvalidAuthorizationSpecificationError,
                'pg_hba.conf rejects connection'):
            await self.cluster.connect(
                user='reject_user', database='postgres', loop=self.loop)

    async def test_auth_password_cleartext(self):
        conn = await self.cluster.connect(
            user='password_user', database='postgres',
            password='correctpassword', loop=self.loop)
        await conn.close()

        with self.assertRaisesRegex(
                asyncpg.InvalidPasswordError,
                'password authentication failed for user "password_user"'):
            await self.cluster.connect(
                user='password_user', database='postgres',
                password='wrongpassword', loop=self.loop)

    async def test_auth_password_md5(self):
        conn = await self.cluster.connect(
            user='md5_user', database='postgres', password='correctpassword',
            loop=self.loop)
        await conn.close()

        with self.assertRaisesRegex(
                asyncpg.InvalidPasswordError,
                'password authentication failed for user "md5_user"'):
            await self.cluster.connect(
                user='md5_user', database='postgres', password='wrongpassword',
                loop=self.loop)

    async def test_auth_unsupported(self):
        pass


class TestConnectParams(unittest.TestCase):

    TESTS = [
        {
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123'
            },
            'result': (['host'], 123, {
                'user': 'user',
                'password': 'passw',
                'database': 'testdb'})
        },

        {
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123'
            },

            'host': 'host2',
            'port': '456',
            'user': 'user2',
            'password': 'passw2',
            'database': 'db2',

            'result': (['host2'], 456, {
                'user': 'user2',
                'password': 'passw2',
                'database': 'db2'})
        },

        {
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123'
            },

            'dsn': 'postgres://user3:123123@localhost/abcdef',

            'host': 'host2',
            'port': '456',
            'user': 'user2',
            'password': 'passw2',
            'database': 'db2',

            'result': (['host2'], 456, {
                'user': 'user2',
                'password': 'passw2',
                'database': 'db2'})
        },

        {
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123'
            },

            'dsn': 'postgres://user3:123123@localhost:5555/abcdef',

            'result': (['localhost'], 5555, {
                'user': 'user3',
                'password': '123123',
                'database': 'abcdef'})
        },

        {
            'dsn': 'postgres://user3:123123@localhost:5555/abcdef',
            'result': (['localhost'], 5555, {
                'user': 'user3',
                'password': '123123',
                'database': 'abcdef'})
        },

        {
            'dsn': 'postgresql://user3:123123@localhost:5555/'
                   'abcdef?param=sss&param=123&host=testhost&user=testuser'
                   '&port=2222&database=testdb',
            'host': '127.0.0.1',
            'port': '888',
            'user': 'me',
            'password': 'ask',
            'database': 'db',
            'result': (['127.0.0.1'], 888, {
                'param': '123',
                'user': 'me',
                'password': 'ask',
                'database': 'db'})
        },

        {
            'dsn': 'postgresql:///dbname?host=/unix_sock/test&user=spam',
            'result': (['/unix_sock/test'], 5432, {
                'user': 'spam',
                'database': 'dbname'})
        },

        {
            'dsn': 'pq:///dbname?host=/unix_sock/test&user=spam',
            'error': (ValueError, 'invalid DSN')
        },
    ]

    @contextlib.contextmanager
    def environ(self, **kwargs):
        old_vals = {}
        for key in kwargs:
            if key in os.environ:
                old_vals[key] = os.environ[key]

        for key, val in kwargs.items():
            if val is None:
                if key in os.environ:
                    del os.environ[key]
            else:
                os.environ[key] = val

        try:
            yield
        finally:
            for key in kwargs:
                if key in os.environ:
                    del os.environ[key]
            for key, val in old_vals.items():
                os.environ[key] = val

    def run_testcase(self, testcase):
        env = testcase.get('env', {})
        test_env = {'PGHOST': None, 'PGPORT': None,
                    'PGUSER': None, 'PGPASSWORD': None,
                    'PGDATABASE': None}
        test_env.update(env)

        dsn = testcase.get('dsn')
        opts = testcase.get('opts', {})
        user = testcase.get('user')
        port = testcase.get('port')
        host = testcase.get('host')
        password = testcase.get('password')
        database = testcase.get('database')

        expected = testcase.get('result')
        expected_error = testcase.get('error')
        if expected is None and expected_error is None:
            raise RuntimeError(
                'invalid test case: either "result" or "error" key '
                'has to be specified')
        if expected is not None and expected_error is not None:
            raise RuntimeError(
                'invalid test case: either "result" or "error" key '
                'has to be specified, got both')

        with contextlib.ExitStack() as es:
            es.enter_context(self.subTest(dsn=dsn, opts=opts, env=env))
            es.enter_context(self.environ(**test_env))

            if expected_error:
                es.enter_context(self.assertRaisesRegex(*expected_error))

            result = _parse_connect_params(
                dsn=dsn, host=host, port=port, user=user, password=password,
                database=database, opts=opts)

        if expected is not None:
            self.assertEqual(expected, result)

    def test_test_connect_params_environ(self):
        self.assertNotIn('AAAAAAAAAA123', os.environ)
        self.assertNotIn('AAAAAAAAAA456', os.environ)
        self.assertNotIn('AAAAAAAAAA789', os.environ)

        try:

            os.environ['AAAAAAAAAA456'] = '123'
            os.environ['AAAAAAAAAA789'] = '123'

            with self.environ(AAAAAAAAAA123='1',
                              AAAAAAAAAA456='2',
                              AAAAAAAAAA789=None):

                self.assertEqual(os.environ['AAAAAAAAAA123'], '1')
                self.assertEqual(os.environ['AAAAAAAAAA456'], '2')
                self.assertNotIn('AAAAAAAAAA789', os.environ)

            self.assertNotIn('AAAAAAAAAA123', os.environ)
            self.assertEqual(os.environ['AAAAAAAAAA456'], '123')
            self.assertEqual(os.environ['AAAAAAAAAA789'], '123')

        finally:
            for key in {'AAAAAAAAAA123', 'AAAAAAAAAA456', 'AAAAAAAAAA789'}:
                if key in os.environ:
                    del os.environ[key]

    def test_test_connect_params_run_testcase(self):
        with self.environ(PGPORT='777'):
            self.run_testcase({
                'env': {
                    'PGUSER': '__test__'
                },
                'host': 'abc',
                'result': (['abc'], 5432, {'user': '__test__'})
            })

        with self.assertRaises(AssertionError):
            self.run_testcase({
                'env': {
                    'PGUSER': '__test__'
                },
                'host': 'abc',
                'result': (['abc'], 5432, {'user': 'wrong_user'})
            })

    def test_connect_params(self):
        for testcase in self.TESTS:
            self.run_testcase(testcase)
