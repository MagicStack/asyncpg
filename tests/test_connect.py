# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncpg
import contextlib
import os
import unittest

from asyncpg import _testbase as tb
from asyncpg.connection import _parse_connect_params


class TestConnect(tb.ConnectedTestCase):

    async def test_connect_1(self):
        with self.assertRaisesRegex(
                Exception, 'role "__does_not_exist__" does not exist'):
            await asyncpg.connect(user="__does_not_exist__", loop=self.loop)


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
