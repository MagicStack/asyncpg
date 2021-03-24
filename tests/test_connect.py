# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import contextlib
import gc
import ipaddress
import os
import platform
import ssl
import stat
import tempfile
import textwrap
import unittest
import weakref

import asyncpg
from asyncpg import _testbase as tb
from asyncpg import connection
from asyncpg import connect_utils
from asyncpg import cluster as pg_cluster
from asyncpg import exceptions
from asyncpg.connect_utils import SSLMode
from asyncpg.serverversion import split_server_version_string

_system = platform.uname().system


CERTS = os.path.join(os.path.dirname(__file__), 'certs')
SSL_CA_CERT_FILE = os.path.join(CERTS, 'ca.cert.pem')
SSL_CERT_FILE = os.path.join(CERTS, 'server.cert.pem')
SSL_KEY_FILE = os.path.join(CERTS, 'server.key.pem')


class TestSettings(tb.ConnectedTestCase):

    async def test_get_settings_01(self):
        self.assertEqual(
            self.con.get_settings().client_encoding,
            'UTF8')

    async def test_server_version_01(self):
        version = self.con.get_server_version()
        version_num = await self.con.fetchval("SELECT current_setting($1)",
                                              'server_version_num', column=0)
        ver_maj = int(version_num[:-4])
        ver_min = int(version_num[-4:-2])
        ver_fix = int(version_num[-2:])

        self.assertEqual(version[:3], (ver_maj, ver_min, ver_fix))

    def test_server_version_02(self):
        versions = [
            ("9.2", (9, 2, 0, 'final', 0),),
            ("Postgres-XL 9.2.1", (9, 2, 1, 'final', 0),),
            ("9.4beta1", (9, 4, 0, 'beta', 1),),
            ("10devel", (10, 0, 0, 'devel', 0),),
            ("10beta2", (10, 0, 0, 'beta', 2),),
            # For PostgreSQL versions >=10 we always
            # set version.minor to 0.
            ("10.1", (10, 0, 1, 'final', 0),),
            ("11.1.2", (11, 0, 1, 'final', 0),),
            ("PostgreSQL 10.1 (Debian 10.1-3)", (10, 0, 1, 'final', 0),),
        ]
        for version, expected in versions:
            result = split_server_version_string(version)
            self.assertEqual(expected, result)


class TestAuthentication(tb.ConnectedTestCase):
    def setUp(self):
        super().setUp()

        if not self.cluster.is_managed():
            self.skipTest('unmanaged cluster')

        methods = [
            ('trust', None),
            ('reject', None),
            ('scram-sha-256', 'correctpassword'),
            ('md5', 'correctpassword'),
            ('password', 'correctpassword'),
        ]

        self.cluster.reset_hba()

        create_script = []
        for method, password in methods:
            if method == 'scram-sha-256' and self.server_version.major < 10:
                continue

            username = method.replace('-', '_')

            # if this is a SCRAM password, we need to set the encryption method
            # to "scram-sha-256" in order to properly hash the password
            if method == 'scram-sha-256':
                create_script.append(
                    "SET password_encryption = 'scram-sha-256';"
                )

            create_script.append(
                'CREATE ROLE {}_user WITH LOGIN{};'.format(
                    username,
                    ' PASSWORD {!r}'.format(password) if password else ''
                )
            )

            # to be courteous to the MD5 test, revert back to MD5 after the
            # scram-sha-256 password is set
            if method == 'scram-sha-256':
                create_script.append(
                    "SET password_encryption = 'md5';"
                )

            if _system != 'Windows':
                self.cluster.add_hba_entry(
                    type='local',
                    database='postgres', user='{}_user'.format(username),
                    auth_method=method)

            self.cluster.add_hba_entry(
                type='host', address=ipaddress.ip_network('127.0.0.0/24'),
                database='postgres', user='{}_user'.format(username),
                auth_method=method)

            self.cluster.add_hba_entry(
                type='host', address=ipaddress.ip_network('::1/128'),
                database='postgres', user='{}_user'.format(username),
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
            'scram-sha-256',
            'md5',
            'password',
        ]

        drop_script = []
        for method in methods:
            if method == 'scram-sha-256' and self.server_version.major < 10:
                continue

            username = method.replace('-', '_')

            drop_script.append('DROP ROLE {}_user;'.format(username))

        drop_script = '\n'.join(drop_script)
        self.loop.run_until_complete(self.con.execute(drop_script))

        super().tearDown()

    async def _try_connect(self, **kwargs):
        # On Windows the server sometimes just closes
        # the connection sooner than we receive the
        # actual error.
        if _system == 'Windows':
            for tried in range(3):
                try:
                    return await self.connect(**kwargs)
                except asyncpg.ConnectionDoesNotExistError:
                    pass

        return await self.connect(**kwargs)

    async def test_auth_bad_user(self):
        with self.assertRaises(
                asyncpg.InvalidAuthorizationSpecificationError):
            await self._try_connect(user='__nonexistent__')

    async def test_auth_trust(self):
        conn = await self.connect(user='trust_user')
        await conn.close()

    async def test_auth_reject(self):
        with self.assertRaisesRegex(
                asyncpg.InvalidAuthorizationSpecificationError,
                'pg_hba.conf rejects connection'):
            await self._try_connect(user='reject_user')

    async def test_auth_password_cleartext(self):
        conn = await self.connect(
            user='password_user',
            password='correctpassword')
        await conn.close()

        with self.assertRaisesRegex(
                asyncpg.InvalidPasswordError,
                'password authentication failed for user "password_user"'):
            await self._try_connect(
                user='password_user',
                password='wrongpassword')

    async def test_auth_password_cleartext_callable(self):
        def get_correctpassword():
            return 'correctpassword'

        def get_wrongpassword():
            return 'wrongpassword'

        conn = await self.connect(
            user='password_user',
            password=get_correctpassword)
        await conn.close()

        with self.assertRaisesRegex(
                asyncpg.InvalidPasswordError,
                'password authentication failed for user "password_user"'):
            await self._try_connect(
                user='password_user',
                password=get_wrongpassword)

    async def test_auth_password_cleartext_callable_coroutine(self):
        async def get_correctpassword():
            return 'correctpassword'

        async def get_wrongpassword():
            return 'wrongpassword'

        conn = await self.connect(
            user='password_user',
            password=get_correctpassword)
        await conn.close()

        with self.assertRaisesRegex(
                asyncpg.InvalidPasswordError,
                'password authentication failed for user "password_user"'):
            await self._try_connect(
                user='password_user',
                password=get_wrongpassword)

    async def test_auth_password_md5(self):
        conn = await self.connect(
            user='md5_user', password='correctpassword')
        await conn.close()

        with self.assertRaisesRegex(
                asyncpg.InvalidPasswordError,
                'password authentication failed for user "md5_user"'):
            await self._try_connect(
                user='md5_user', password='wrongpassword')

    async def test_auth_password_scram_sha_256(self):
        # scram is only supported in PostgreSQL 10 and above
        if self.server_version.major < 10:
            return

        conn = await self.connect(
            user='scram_sha_256_user', password='correctpassword')
        await conn.close()

        with self.assertRaisesRegex(
                asyncpg.InvalidPasswordError,
                'password authentication failed for user "scram_sha_256_user"'
        ):
            await self._try_connect(
                user='scram_sha_256_user', password='wrongpassword')

        # various SASL prep tests
        # first ensure that password are being hashed for SCRAM-SHA-256
        await self.con.execute("SET password_encryption = 'scram-sha-256';")
        alter_password = "ALTER ROLE scram_sha_256_user PASSWORD E{!r};"
        passwords = [
            'nonascii\u1680space',  # C.1.2
            'common\u1806nothing',  # B.1
            'ab\ufb01c',            # normalization
            'ab\u007fc',            # C.2.1
            'ab\u206ac',            # C.2.2, C.6
            'ab\ue000c',            # C.3, C.5
            'ab\ufdd0c',            # C.4
            'ab\u2ff0c',            # C.7
            'ab\u2000c',            # C.8
            'ab\ue0001',            # C.9
        ]

        # ensure the passwords that go through SASLprep work
        for password in passwords:
            # update the password
            await self.con.execute(alter_password.format(password))
            # test to see that passwords are properly SASL prepped
            conn = await self.connect(
                user='scram_sha_256_user', password=password)
            await conn.close()

        alter_password = \
            "ALTER ROLE scram_sha_256_user PASSWORD 'correctpassword';"
        await self.con.execute(alter_password)
        await self.con.execute("SET password_encryption = 'md5';")

    async def test_auth_unsupported(self):
        pass


class TestConnectParams(tb.TestCase):

    TESTS = [
        {
            'name': 'all_env_default_ssl',
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123'
            },
            'result': ([('host', 123)], {
                'user': 'user',
                'password': 'passw',
                'database': 'testdb',
                'ssl': True,
                'sslmode': SSLMode.prefer})
        },

        {
            'name': 'params_override_env',
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

            'result': ([('host2', 456)], {
                'user': 'user2',
                'password': 'passw2',
                'database': 'db2'})
        },

        {
            'name': 'params_override_env_and_dsn',
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123',
                'PGSSLMODE': 'allow'
            },

            'dsn': 'postgres://user3:123123@localhost/abcdef',

            'host': 'host2',
            'port': '456',
            'user': 'user2',
            'password': 'passw2',
            'database': 'db2',
            'ssl': False,

            'result': ([('host2', 456)], {
                'user': 'user2',
                'password': 'passw2',
                'database': 'db2',
                'sslmode': SSLMode.disable,
                'ssl': False})
        },

        {
            'name': 'dsn_overrides_env_partially',
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123',
                'PGSSLMODE': 'allow'
            },

            'dsn': 'postgres://user3:123123@localhost:5555/abcdef',

            'result': ([('localhost', 5555)], {
                'user': 'user3',
                'password': '123123',
                'database': 'abcdef',
                'ssl': True,
                'sslmode': SSLMode.allow})
        },

        {
            'name': 'params_override_env_and_dsn_ssl_prefer',
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123',
                'PGSSLMODE': 'prefer'
            },

            'dsn': 'postgres://user3:123123@localhost/abcdef',

            'host': 'host2',
            'port': '456',
            'user': 'user2',
            'password': 'passw2',
            'database': 'db2',
            'ssl': False,

            'result': ([('host2', 456)], {
                'user': 'user2',
                'password': 'passw2',
                'database': 'db2',
                'sslmode': SSLMode.disable,
                'ssl': False})
        },

        {
            'name': 'dsn_overrides_env_partially_ssl_prefer',
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123',
                'PGSSLMODE': 'prefer'
            },

            'dsn': 'postgres://user3:123123@localhost:5555/abcdef',

            'result': ([('localhost', 5555)], {
                'user': 'user3',
                'password': '123123',
                'database': 'abcdef',
                'ssl': True,
                'sslmode': SSLMode.prefer})
        },

        {
            'name': 'dsn_only',
            'dsn': 'postgres://user3:123123@localhost:5555/abcdef',
            'result': ([('localhost', 5555)], {
                'user': 'user3',
                'password': '123123',
                'database': 'abcdef'})
        },

        {
            'name': 'dsn_only_multi_host',
            'dsn': 'postgresql://user@host1,host2/db',
            'result': ([('host1', 5432), ('host2', 5432)], {
                'database': 'db',
                'user': 'user',
            })
        },

        {
            'name': 'dsn_only_multi_host_and_port',
            'dsn': 'postgresql://user@host1:1111,host2:2222/db',
            'result': ([('host1', 1111), ('host2', 2222)], {
                'database': 'db',
                'user': 'user',
            })
        },

        {
            'name': 'dsn_combines_env_multi_host',
            'env': {
                'PGHOST': 'host1:1111,host2:2222',
                'PGUSER': 'foo',
            },
            'dsn': 'postgresql:///db',
            'result': ([('host1', 1111), ('host2', 2222)], {
                'database': 'db',
                'user': 'foo',
            })
        },

        {
            'name': 'dsn_multi_host_combines_env',
            'env': {
                'PGUSER': 'foo',
            },
            'dsn': 'postgresql:///db?host=host1:1111,host2:2222',
            'result': ([('host1', 1111), ('host2', 2222)], {
                'database': 'db',
                'user': 'foo',
            })
        },

        {
            'name': 'params_multi_host_dsn_env_mix',
            'env': {
                'PGUSER': 'foo',
            },
            'dsn': 'postgresql:///db',
            'host': ['host1', 'host2'],
            'result': ([('host1', 5432), ('host2', 5432)], {
                'database': 'db',
                'user': 'foo',
            })
        },

        {
            'name': 'params_combine_dsn_settings_override_and_ssl',
            'dsn': 'postgresql://user3:123123@localhost:5555/'
                   'abcdef?param=sss&param=123&host=testhost&user=testuser'
                   '&port=2222&database=testdb&sslmode=require',
            'host': '127.0.0.1',
            'port': '888',
            'user': 'me',
            'password': 'ask',
            'database': 'db',
            'result': ([('127.0.0.1', 888)], {
                'server_settings': {'param': '123'},
                'user': 'me',
                'password': 'ask',
                'database': 'db',
                'ssl': True,
                'sslmode': SSLMode.require})
        },

        {
            'name': 'params_settings_and_ssl_override_dsn',
            'dsn': 'postgresql://user3:123123@localhost:5555/'
                   'abcdef?param=sss&param=123&host=testhost&user=testuser'
                   '&port=2222&database=testdb&sslmode=disable',
            'host': '127.0.0.1',
            'port': '888',
            'user': 'me',
            'password': 'ask',
            'database': 'db',
            'server_settings': {'aa': 'bb'},
            'ssl': True,
            'result': ([('127.0.0.1', 888)], {
                'server_settings': {'aa': 'bb', 'param': '123'},
                'user': 'me',
                'password': 'ask',
                'database': 'db',
                'sslmode': SSLMode.verify_full,
                'ssl': True})
        },

        {
            'name': 'dsn_only_unix',
            'dsn': 'postgresql:///dbname?host=/unix_sock/test&user=spam',
            'result': ([os.path.join('/unix_sock/test', '.s.PGSQL.5432')], {
                'user': 'spam',
                'database': 'dbname'})
        },

        {
            'name': 'dsn_only_quoted',
            'dsn': 'postgresql://us%40r:p%40ss@h%40st1,h%40st2:543%33/d%62',
            'result': (
                [('h@st1', 5432), ('h@st2', 5433)],
                {
                    'user': 'us@r',
                    'password': 'p@ss',
                    'database': 'db',
                }
            )
        },

        {
            'name': 'dsn_only_unquoted_host',
            'dsn': 'postgresql://user:p@ss@host/db',
            'result': (
                [('ss@host', 5432)],
                {
                    'user': 'user',
                    'password': 'p',
                    'database': 'db',
                }
            )
        },

        {
            'name': 'dsn_only_quoted_params',
            'dsn': 'postgresql:///d%62?user=us%40r&host=h%40st&port=543%33',
            'result': (
                [('h@st', 5433)],
                {
                    'user': 'us@r',
                    'database': 'db',
                }
            )
        },

        {
            'name': 'dsn_only_illegal_protocol',
            'dsn': 'pq:///dbname?host=/unix_sock/test&user=spam',
            'error': (ValueError, 'invalid DSN')
        },
        {
            'name': 'dsn_params_ports_mismatch_dsn_multi_hosts',
            'dsn': 'postgresql://host1,host2,host3/db',
            'port': [111, 222],
            'error': (
                exceptions.InterfaceError,
                'could not match 2 port numbers to 3 hosts'
            )
        },
        {
            'name': 'dsn_only_quoted_unix_host_port_in_params',
            'dsn': 'postgres://user@?port=56226&host=%2Ftmp',
            'result': (
                [os.path.join('/tmp', '.s.PGSQL.56226')],
                {
                    'user': 'user',
                    'database': 'user',
                    'sslmode': SSLMode.disable,
                    'ssl': None
                }
            )
        },
        {
            'name': 'dsn_only_cloudsql',
            'dsn': 'postgres:///db?host=/cloudsql/'
                   'project:region:instance-name&user=spam',
            'result': (
                [os.path.join(
                    '/cloudsql/project:region:instance-name',
                    '.s.PGSQL.5432'
                )], {
                    'user': 'spam',
                    'database': 'db'
                }
            )
        },
        {
            'name': 'dsn_only_cloudsql_unix_and_tcp',
            'dsn': 'postgres:///db?host=127.0.0.1:5432,/cloudsql/'
                   'project:region:instance-name,localhost:5433&user=spam',
            'result': (
                [
                    ('127.0.0.1', 5432),
                    os.path.join(
                        '/cloudsql/project:region:instance-name',
                        '.s.PGSQL.5432'
                    ),
                    ('localhost', 5433)
                ], {
                    'user': 'spam',
                    'database': 'db',
                    'ssl': True,
                    'sslmode': SSLMode.prefer,
                }
            )
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
                    'PGDATABASE': None, 'PGSSLMODE': None}
        test_env.update(env)

        dsn = testcase.get('dsn')
        user = testcase.get('user')
        port = testcase.get('port')
        host = testcase.get('host')
        password = testcase.get('password')
        passfile = testcase.get('passfile')
        database = testcase.get('database')
        sslmode = testcase.get('ssl')
        server_settings = testcase.get('server_settings')

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
            es.enter_context(self.subTest(dsn=dsn, env=env))
            es.enter_context(self.environ(**test_env))

            if expected_error:
                es.enter_context(self.assertRaisesRegex(*expected_error))

            addrs, params = connect_utils._parse_connect_dsn_and_args(
                dsn=dsn, host=host, port=port, user=user, password=password,
                passfile=passfile, database=database, ssl=sslmode,
                connect_timeout=None, server_settings=server_settings)

            params = {
                k: v for k, v in params._asdict().items()
                if v is not None or (expected is not None and k in expected[1])
            }

            if isinstance(params.get('ssl'), ssl.SSLContext):
                params['ssl'] = True

            result = (addrs, params)

        if expected is not None:
            if 'ssl' not in expected[1]:
                # Avoid the hassle of specifying the default SSL mode
                # unless explicitly tested for.
                params.pop('ssl', None)
                params.pop('sslmode', None)

            self.assertEqual(expected, result, 'Testcase: {}'.format(testcase))

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
                'result': (
                    [('abc', 5432)],
                    {'user': '__test__', 'database': '__test__'}
                )
            })

    def test_connect_params(self):
        for testcase in self.TESTS:
            self.run_testcase(testcase)

    def test_connect_pgpass_regular(self):
        passfile = tempfile.NamedTemporaryFile('w+t', delete=False)
        passfile.write(textwrap.dedent(R'''
            abc:*:*:user:password from pgpass for user@abc
            localhost:*:*:*:password from pgpass for localhost
            cde:5433:*:*:password from pgpass for cde:5433

            *:*:*:testuser:password from pgpass for testuser
            *:*:testdb:*:password from pgpass for testdb
            # comment
            *:*:test\:db:test\\:password from pgpass with escapes
        '''))
        passfile.close()
        os.chmod(passfile.name, stat.S_IWUSR | stat.S_IRUSR)

        try:
            # passfile path in env
            self.run_testcase({
                'env': {
                    'PGPASSFILE': passfile.name
                },
                'host': 'abc',
                'user': 'user',
                'database': 'db',
                'result': (
                    [('abc', 5432)],
                    {
                        'password': 'password from pgpass for user@abc',
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

            # passfile path as explicit arg
            self.run_testcase({
                'host': 'abc',
                'user': 'user',
                'database': 'db',
                'passfile': passfile.name,
                'result': (
                    [('abc', 5432)],
                    {
                        'password': 'password from pgpass for user@abc',
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

            # passfile path in dsn
            self.run_testcase({
                'dsn': 'postgres://user@abc/db?passfile={}'.format(
                    passfile.name),
                'result': (
                    [('abc', 5432)],
                    {
                        'password': 'password from pgpass for user@abc',
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

            self.run_testcase({
                'host': 'localhost',
                'user': 'user',
                'database': 'db',
                'passfile': passfile.name,
                'result': (
                    [('localhost', 5432)],
                    {
                        'password': 'password from pgpass for localhost',
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

            if _system != 'Windows':
                # unix socket gets normalized as localhost
                self.run_testcase({
                    'host': '/tmp',
                    'user': 'user',
                    'database': 'db',
                    'passfile': passfile.name,
                    'result': (
                        ['/tmp/.s.PGSQL.5432'],
                        {
                            'password': 'password from pgpass for localhost',
                            'user': 'user',
                            'database': 'db',
                        }
                    )
                })

            # port matching (also tests that `:` can be part of password)
            self.run_testcase({
                'host': 'cde',
                'port': 5433,
                'user': 'user',
                'database': 'db',
                'passfile': passfile.name,
                'result': (
                    [('cde', 5433)],
                    {
                        'password': 'password from pgpass for cde:5433',
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

            # user matching
            self.run_testcase({
                'host': 'def',
                'user': 'testuser',
                'database': 'db',
                'passfile': passfile.name,
                'result': (
                    [('def', 5432)],
                    {
                        'password': 'password from pgpass for testuser',
                        'user': 'testuser',
                        'database': 'db',
                    }
                )
            })

            # database matching
            self.run_testcase({
                'host': 'efg',
                'user': 'user',
                'database': 'testdb',
                'passfile': passfile.name,
                'result': (
                    [('efg', 5432)],
                    {
                        'password': 'password from pgpass for testdb',
                        'user': 'user',
                        'database': 'testdb',
                    }
                )
            })

            # test escaping
            self.run_testcase({
                'host': 'fgh',
                'user': R'test\\',
                'database': R'test\:db',
                'passfile': passfile.name,
                'result': (
                    [('fgh', 5432)],
                    {
                        'password': 'password from pgpass with escapes',
                        'user': R'test\\',
                        'database': R'test\:db',
                    }
                )
            })

        finally:
            os.unlink(passfile.name)

    @unittest.skipIf(_system == 'Windows', 'no mode checking on Windows')
    def test_connect_pgpass_badness_mode(self):
        # Verify that .pgpass permissions are checked
        with tempfile.NamedTemporaryFile('w+t') as passfile:
            os.chmod(passfile.name,
                     stat.S_IWUSR | stat.S_IRUSR | stat.S_IWGRP | stat.S_IRGRP)

            with self.assertWarnsRegex(
                    UserWarning,
                    'password file .* has group or world access'):
                self.run_testcase({
                    'host': 'abc',
                    'user': 'user',
                    'database': 'db',
                    'passfile': passfile.name,
                    'result': (
                        [('abc', 5432)],
                        {
                            'user': 'user',
                            'database': 'db',
                        }
                    )
                })

    def test_connect_pgpass_badness_non_file(self):
        # Verify warnings when .pgpass is not a file
        with tempfile.TemporaryDirectory() as passfile:
            with self.assertWarnsRegex(
                    UserWarning,
                    'password file .* is not a plain file'):
                self.run_testcase({
                    'host': 'abc',
                    'user': 'user',
                    'database': 'db',
                    'passfile': passfile,
                    'result': (
                        [('abc', 5432)],
                        {
                            'user': 'user',
                            'database': 'db',
                        }
                    )
                })

    def test_connect_pgpass_nonexistent(self):
        # nonexistent passfile is OK
        self.run_testcase({
            'host': 'abc',
            'user': 'user',
            'database': 'db',
            'passfile': 'totally nonexistent',
            'result': (
                [('abc', 5432)],
                {
                    'user': 'user',
                    'database': 'db',
                }
            )
        })

    @unittest.skipIf(_system == 'Windows', 'no mode checking on Windows')
    def test_connect_pgpass_inaccessible_file(self):
        with tempfile.NamedTemporaryFile('w+t') as passfile:
            os.chmod(passfile.name, stat.S_IWUSR)

            # nonexistent passfile is OK
            self.run_testcase({
                'host': 'abc',
                'user': 'user',
                'database': 'db',
                'passfile': passfile.name,
                'result': (
                    [('abc', 5432)],
                    {
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

    @unittest.skipIf(_system == 'Windows', 'no mode checking on Windows')
    def test_connect_pgpass_inaccessible_directory(self):
        with tempfile.TemporaryDirectory() as passdir:
            with tempfile.NamedTemporaryFile('w+t', dir=passdir) as passfile:
                os.chmod(passdir, stat.S_IWUSR)

                try:
                    # nonexistent passfile is OK
                    self.run_testcase({
                        'host': 'abc',
                        'user': 'user',
                        'database': 'db',
                        'passfile': passfile.name,
                        'result': (
                            [('abc', 5432)],
                            {
                                'user': 'user',
                                'database': 'db',
                            }
                        )
                    })
                finally:
                    os.chmod(passdir, stat.S_IRWXU)

    async def test_connect_args_validation(self):
        for val in {-1, 'a', True, False, 0}:
            with self.assertRaisesRegex(ValueError, 'greater than 0'):
                await asyncpg.connect(command_timeout=val)

        for arg in {'max_cacheable_statement_size',
                    'max_cached_statement_lifetime',
                    'statement_cache_size'}:
            for val in {None, -1, True, False}:
                with self.assertRaisesRegex(ValueError, 'greater or equal'):
                    await asyncpg.connect(**{arg: val})


class TestConnection(tb.ConnectedTestCase):

    async def test_connection_isinstance(self):
        self.assertTrue(isinstance(self.con, connection.Connection))
        self.assertTrue(isinstance(self.con, object))
        self.assertFalse(isinstance(self.con, list))

    async def test_connection_use_after_close(self):
        def check():
            return self.assertRaisesRegex(asyncpg.InterfaceError,
                                          'connection is closed')

        await self.con.close()

        with check():
            await self.con.add_listener('aaa', lambda: None)

        with check():
            self.con.transaction()

        with check():
            await self.con.executemany('SELECT 1', [])

        with check():
            await self.con.set_type_codec('aaa', encoder=None, decoder=None)

        with check():
            await self.con.set_builtin_type_codec('aaa', codec_name='aaa')

        for meth in ('execute', 'fetch', 'fetchval', 'fetchrow',
                     'prepare', 'cursor'):

            with check():
                await getattr(self.con, meth)('SELECT 1')

        with check():
            await self.con.reset()

    @unittest.skipIf(os.environ.get('PGHOST'), 'unmanaged cluster')
    async def test_connection_ssl_to_no_ssl_server(self):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ssl_context.load_verify_locations(SSL_CA_CERT_FILE)

        with self.assertRaisesRegex(ConnectionError, 'rejected SSL'):
            await self.connect(
                host='localhost',
                user='ssl_user',
                ssl=ssl_context)

    @unittest.skipIf(os.environ.get('PGHOST'), 'unmanaged cluster')
    async def test_connection_sslmode_no_ssl_server(self):
        async def verify_works(sslmode):
            con = None
            try:
                con = await self.connect(
                    dsn='postgresql://foo/?sslmode=' + sslmode,
                    host='localhost')
                self.assertEqual(await con.fetchval('SELECT 42'), 42)
                self.assertFalse(con._protocol.is_ssl)
            finally:
                if con:
                    await con.close()

        async def verify_fails(sslmode):
            con = None
            try:
                with self.assertRaises(ConnectionError):
                    con = await self.connect(
                        dsn='postgresql://foo/?sslmode=' + sslmode,
                        host='localhost')
                    await con.fetchval('SELECT 42')
            finally:
                if con:
                    await con.close()

        await verify_works('disable')
        await verify_works('allow')
        await verify_works('prefer')
        await verify_fails('require')
        await verify_fails('verify-ca')
        await verify_fails('verify-full')

    async def test_connection_implicit_host(self):
        conn_spec = self.get_connection_spec()
        con = await asyncpg.connect(
            port=conn_spec.get('port'),
            database=conn_spec.get('database'),
            user=conn_spec.get('user'))
        await con.close()


class BaseTestSSLConnection(tb.ConnectedTestCase):
    @classmethod
    def get_server_settings(cls):
        conf = super().get_server_settings()
        conf.update({
            'ssl': 'on',
            'ssl_cert_file': SSL_CERT_FILE,
            'ssl_key_file': SSL_KEY_FILE,
        })

        return conf

    @classmethod
    def setup_cluster(cls):
        cls.cluster = cls.new_cluster(pg_cluster.TempCluster)
        cls.start_cluster(
            cls.cluster, server_settings=cls.get_server_settings())

    def setUp(self):
        super().setUp()

        self.cluster.reset_hba()

        create_script = []
        create_script.append('CREATE ROLE ssl_user WITH LOGIN;')

        self._add_hba_entry()

        # Put hba changes into effect
        self.cluster.reload()

        create_script = '\n'.join(create_script)
        self.loop.run_until_complete(self.con.execute(create_script))

    def tearDown(self):
        # Reset cluster's pg_hba.conf since we've meddled with it
        self.cluster.trust_local_connections()

        drop_script = []
        drop_script.append('DROP ROLE ssl_user;')
        drop_script = '\n'.join(drop_script)
        self.loop.run_until_complete(self.con.execute(drop_script))

        super().tearDown()

    def _add_hba_entry(self):
        raise NotImplementedError()


@unittest.skipIf(os.environ.get('PGHOST'), 'unmanaged cluster')
class TestSSLConnection(BaseTestSSLConnection):
    def _add_hba_entry(self):
        self.cluster.add_hba_entry(
            type='hostssl', address=ipaddress.ip_network('127.0.0.0/24'),
            database='postgres', user='ssl_user',
            auth_method='trust')

        self.cluster.add_hba_entry(
            type='hostssl', address=ipaddress.ip_network('::1/128'),
            database='postgres', user='ssl_user',
            auth_method='trust')

    async def test_ssl_connection_custom_context(self):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ssl_context.load_verify_locations(SSL_CA_CERT_FILE)

        con = await self.connect(
            host='localhost',
            user='ssl_user',
            ssl=ssl_context)

        try:
            self.assertEqual(await con.fetchval('SELECT 42'), 42)

            with self.assertRaises(asyncio.TimeoutError):
                await con.execute('SELECT pg_sleep(5)', timeout=0.5)

            self.assertEqual(await con.fetchval('SELECT 43'), 43)
        finally:
            await con.close()

    async def test_ssl_connection_sslmode(self):
        async def verify_works(sslmode, *, host='localhost'):
            con = None
            try:
                con = await self.connect(
                    dsn='postgresql://foo/?sslmode=' + sslmode,
                    host=host,
                    user='ssl_user')
                self.assertEqual(await con.fetchval('SELECT 42'), 42)
                self.assertTrue(con._protocol.is_ssl)
            finally:
                if con:
                    await con.close()

        async def verify_fails(sslmode, *, host='localhost',
                               exn_type=ssl.SSLError):
            # XXX: uvloop artifact
            old_handler = self.loop.get_exception_handler()
            con = None
            try:
                self.loop.set_exception_handler(lambda *args: None)
                with self.assertRaises(exn_type):
                    con = await self.connect(
                        dsn='postgresql://foo/?sslmode=' + sslmode,
                        host=host,
                        user='ssl_user')
                    await con.fetchval('SELECT 42')
            finally:
                if con:
                    await con.close()
                self.loop.set_exception_handler(old_handler)

        invalid_auth_err = asyncpg.InvalidAuthorizationSpecificationError
        await verify_fails('disable', exn_type=invalid_auth_err)
        await verify_works('allow')
        await verify_works('prefer')
        await verify_works('require')
        await verify_fails('verify-ca')
        await verify_fails('verify-full')

        orig_create_default_context = ssl.create_default_context
        try:
            def custom_create_default_context(*args, **kwargs):
                ctx = orig_create_default_context(*args, **kwargs)
                ctx.load_verify_locations(cafile=SSL_CA_CERT_FILE)
                return ctx
            ssl.create_default_context = custom_create_default_context
            await verify_works('verify-ca')
            await verify_works('verify-ca', host='127.0.0.1')
            await verify_works('verify-full')
            await verify_fails('verify-full', host='127.0.0.1',
                               exn_type=ssl.CertificateError)
        finally:
            ssl.create_default_context = orig_create_default_context

    async def test_ssl_connection_default_context(self):
        # XXX: uvloop artifact
        old_handler = self.loop.get_exception_handler()
        try:
            self.loop.set_exception_handler(lambda *args: None)
            with self.assertRaisesRegex(ssl.SSLError, 'verify failed'):
                await self.connect(
                    host='localhost',
                    user='ssl_user',
                    ssl=True)
        finally:
            self.loop.set_exception_handler(old_handler)

    async def test_ssl_connection_pool(self):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ssl_context.load_verify_locations(SSL_CA_CERT_FILE)

        pool = await self.create_pool(
            host='localhost',
            user='ssl_user',
            database='postgres',
            min_size=5,
            max_size=10,
            ssl=ssl_context)

        async def worker():
            async with pool.acquire() as con:
                self.assertEqual(await con.fetchval('SELECT 42'), 42)

                with self.assertRaises(asyncio.TimeoutError):
                    await con.execute('SELECT pg_sleep(5)', timeout=0.5)

                self.assertEqual(await con.fetchval('SELECT 43'), 43)

        tasks = [worker() for _ in range(100)]
        await asyncio.gather(*tasks)
        await pool.close()

    async def test_executemany_uvloop_ssl_issue_700(self):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ssl_context.load_verify_locations(SSL_CA_CERT_FILE)

        con = await self.connect(
            host='localhost',
            user='ssl_user',
            ssl=ssl_context)

        try:
            await con.execute('CREATE TABLE test_many (v int)')
            await con.executemany(
                'INSERT INTO test_many VALUES ($1)',
                [(x + 1,) for x in range(100)]
            )
            self.assertEqual(
                await con.fetchval('SELECT sum(v) FROM test_many'), 5050
            )
        finally:
            try:
                await con.execute('DROP TABLE test_many')
            finally:
                await con.close()


@unittest.skipIf(os.environ.get('PGHOST'), 'unmanaged cluster')
class TestNoSSLConnection(BaseTestSSLConnection):
    def _add_hba_entry(self):
        self.cluster.add_hba_entry(
            type='hostnossl', address=ipaddress.ip_network('127.0.0.0/24'),
            database='postgres', user='ssl_user',
            auth_method='trust')

        self.cluster.add_hba_entry(
            type='hostnossl', address=ipaddress.ip_network('::1/128'),
            database='postgres', user='ssl_user',
            auth_method='trust')

    async def test_nossl_connection_sslmode(self):
        async def verify_works(sslmode, *, host='localhost'):
            con = None
            try:
                con = await self.connect(
                    dsn='postgresql://foo/?sslmode=' + sslmode,
                    host=host,
                    user='ssl_user')
                self.assertEqual(await con.fetchval('SELECT 42'), 42)
                self.assertFalse(con._protocol.is_ssl)
            finally:
                if con:
                    await con.close()

        async def verify_fails(sslmode, *, host='localhost',
                               exn_type=ssl.SSLError):
            # XXX: uvloop artifact
            old_handler = self.loop.get_exception_handler()
            con = None
            try:
                self.loop.set_exception_handler(lambda *args: None)
                with self.assertRaises(exn_type):
                    con = await self.connect(
                        dsn='postgresql://foo/?sslmode=' + sslmode,
                        host=host,
                        user='ssl_user')
                    await con.fetchval('SELECT 42')
            finally:
                if con:
                    await con.close()
                self.loop.set_exception_handler(old_handler)

        invalid_auth_err = asyncpg.InvalidAuthorizationSpecificationError
        await verify_works('disable')
        await verify_works('allow')
        await verify_works('prefer')
        await verify_fails('require', exn_type=invalid_auth_err)
        await verify_fails('verify-ca')
        await verify_fails('verify-full')

    async def test_nossl_connection_prefer_cancel(self):
        con = await self.connect(
            dsn='postgresql://foo/?sslmode=prefer',
            host='localhost',
            user='ssl_user')
        self.assertFalse(con._protocol.is_ssl)
        with self.assertRaises(asyncio.TimeoutError):
            await con.execute('SELECT pg_sleep(5)', timeout=0.5)
        val = await con.fetchval('SELECT 123')
        self.assertEqual(val, 123)

    async def test_nossl_connection_pool(self):
        pool = await self.create_pool(
            host='localhost',
            user='ssl_user',
            database='postgres',
            min_size=5,
            max_size=10,
            ssl='prefer')

        async def worker():
            async with pool.acquire() as con:
                self.assertFalse(con._protocol.is_ssl)
                self.assertEqual(await con.fetchval('SELECT 42'), 42)

                with self.assertRaises(asyncio.TimeoutError):
                    await con.execute('SELECT pg_sleep(5)', timeout=0.5)

                self.assertEqual(await con.fetchval('SELECT 43'), 43)

        tasks = [worker() for _ in range(100)]
        await asyncio.gather(*tasks)
        await pool.close()


class TestConnectionGC(tb.ClusterTestCase):

    async def _run_no_explicit_close_test(self):
        con = await self.connect()
        proto = con._protocol
        conref = weakref.ref(con)
        del con

        gc.collect()
        gc.collect()
        gc.collect()

        self.assertIsNone(conref())
        self.assertTrue(proto.is_closed())

    async def test_no_explicit_close_no_debug(self):
        olddebug = self.loop.get_debug()
        self.loop.set_debug(False)
        try:
            with self.assertWarnsRegex(
                    ResourceWarning,
                    r'unclosed connection.*run in asyncio debug'):
                await self._run_no_explicit_close_test()
        finally:
            self.loop.set_debug(olddebug)

    async def test_no_explicit_close_with_debug(self):
        olddebug = self.loop.get_debug()
        self.loop.set_debug(True)
        try:
            with self.assertWarnsRegex(ResourceWarning,
                                       r'unclosed connection') as rw:
                await self._run_no_explicit_close_test()

            msg = rw.warning.args[0]
            self.assertIn(' created at:\n', msg)
            self.assertIn('in test_no_explicit_close_with_debug', msg)
        finally:
            self.loop.set_debug(olddebug)
