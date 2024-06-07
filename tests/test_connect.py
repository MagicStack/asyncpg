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
import pathlib
import platform
import shutil
import socket
import ssl
import stat
import tempfile
import textwrap
import unittest
import unittest.mock
import urllib.parse
import warnings
import weakref

import asyncpg
from asyncpg import _testbase as tb
from asyncpg import connection as pg_connection
from asyncpg import connect_utils
from asyncpg import cluster as pg_cluster
from asyncpg import exceptions
from asyncpg.connect_utils import SSLMode
from asyncpg.serverversion import split_server_version_string

_system = platform.uname().system


CERTS = os.path.join(os.path.dirname(__file__), 'certs')
SSL_CA_CERT_FILE = os.path.join(CERTS, 'ca.cert.pem')
SSL_CA_CRL_FILE = os.path.join(CERTS, 'ca.crl.pem')
SSL_CERT_FILE = os.path.join(CERTS, 'server.cert.pem')
SSL_KEY_FILE = os.path.join(CERTS, 'server.key.pem')
CLIENT_CA_CERT_FILE = os.path.join(CERTS, 'client_ca.cert.pem')
CLIENT_SSL_CERT_FILE = os.path.join(CERTS, 'client.cert.pem')
CLIENT_SSL_KEY_FILE = os.path.join(CERTS, 'client.key.pem')
CLIENT_SSL_PROTECTED_KEY_FILE = os.path.join(CERTS, 'client.key.protected.pem')

if _system == 'Windows':
    DEFAULT_GSSLIB = 'sspi'
    OTHER_GSSLIB = 'gssapi'
else:
    DEFAULT_GSSLIB = 'gssapi'
    OTHER_GSSLIB = 'sspi'


@contextlib.contextmanager
def mock_dot_postgresql(*, ca=True, crl=False, client=False, protected=False):
    with tempfile.TemporaryDirectory() as temp_dir:
        home = pathlib.Path(temp_dir)
        pg_home = home / '.postgresql'
        pg_home.mkdir()
        if ca:
            shutil.copyfile(SSL_CA_CERT_FILE, pg_home / 'root.crt')
        if crl:
            shutil.copyfile(SSL_CA_CRL_FILE, pg_home / 'root.crl')
        if client:
            shutil.copyfile(CLIENT_SSL_CERT_FILE, pg_home / 'postgresql.crt')
            if protected:
                shutil.copyfile(
                    CLIENT_SSL_PROTECTED_KEY_FILE, pg_home / 'postgresql.key'
                )
            else:
                shutil.copyfile(
                    CLIENT_SSL_KEY_FILE, pg_home / 'postgresql.key'
                )
        with unittest.mock.patch(
            'pathlib.Path.home', unittest.mock.Mock(return_value=home)
        ):
            yield


@contextlib.contextmanager
def mock_no_home_dir():
    with unittest.mock.patch(
        'pathlib.Path.home', unittest.mock.Mock(side_effect=RuntimeError)
    ):
        yield


@contextlib.contextmanager
def mock_dev_null_home_dir():
    with unittest.mock.patch(
        'pathlib.Path.home',
        unittest.mock.Mock(return_value=pathlib.Path('/dev/null')),
    ):
        yield


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
            ("PostgreSQL 11.2-YB-2.7.1.1-b0 on x86_64-pc-linux-gnu, "
             "compiled by gcc (Homebrew gcc 5.5.0_4) 5.5.0, 64-bit",
             (11, 0, 2, "final", 0),),
        ]
        for version, expected in versions:
            result = split_server_version_string(version)
            self.assertEqual(expected, result)


CORRECT_PASSWORD = 'correct\u1680password'


class BaseTestAuthentication(tb.ConnectedTestCase):
    USERS = []

    def setUp(self):
        super().setUp()

        if not self.cluster.is_managed():
            self.skipTest('unmanaged cluster')

        self.cluster.reset_hba()

        create_script = []
        for username, method, password in self.USERS:
            if method == 'scram-sha-256' and self.server_version.major < 10:
                continue

            # if this is a SCRAM password, we need to set the encryption method
            # to "scram-sha-256" in order to properly hash the password
            if method == 'scram-sha-256':
                create_script.append(
                    "SET password_encryption = 'scram-sha-256';"
                )

            create_script.append(
                'CREATE ROLE "{}" WITH LOGIN{};'.format(
                    username,
                    f' PASSWORD E{(password or "")!r}'
                )
            )

            # to be courteous to the MD5 test, revert back to MD5 after the
            # scram-sha-256 password is set
            if method == 'scram-sha-256':
                create_script.append(
                    "SET password_encryption = 'md5';"
                )

            if _system != 'Windows' and method != 'gss':
                self.cluster.add_hba_entry(
                    type='local',
                    database='postgres', user=username,
                    auth_method=method)

            self.cluster.add_hba_entry(
                type='host', address=ipaddress.ip_network('127.0.0.0/24'),
                database='postgres', user=username,
                auth_method=method)

            self.cluster.add_hba_entry(
                type='host', address=ipaddress.ip_network('::1/128'),
                database='postgres', user=username,
                auth_method=method)

        # Put hba changes into effect
        self.cluster.reload()

        create_script = '\n'.join(create_script)
        self.loop.run_until_complete(self.con.execute(create_script))

    def tearDown(self):
        # Reset cluster's pg_hba.conf since we've meddled with it
        self.cluster.trust_local_connections()

        drop_script = []
        for username, method, _ in self.USERS:
            if method == 'scram-sha-256' and self.server_version.major < 10:
                continue

            drop_script.append('DROP ROLE "{}";'.format(username))

        drop_script = '\n'.join(drop_script)
        self.loop.run_until_complete(self.con.execute(drop_script))

        super().tearDown()


class TestAuthentication(BaseTestAuthentication):
    USERS = [
        ('trust_user', 'trust', None),
        ('reject_user', 'reject', None),
        ('scram_sha_256_user', 'scram-sha-256', CORRECT_PASSWORD),
        ('md5_user', 'md5', CORRECT_PASSWORD),
        ('password_user', 'password', CORRECT_PASSWORD),
    ]

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
            password=CORRECT_PASSWORD)
        await conn.close()

        with self.assertRaisesRegex(
                asyncpg.InvalidPasswordError,
                'password authentication failed for user "password_user"'):
            await self._try_connect(
                user='password_user',
                password='wrongpassword')

    async def test_auth_password_cleartext_callable(self):
        def get_correctpassword():
            return CORRECT_PASSWORD

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
            return CORRECT_PASSWORD

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

    async def test_auth_password_cleartext_callable_awaitable(self):
        async def get_correctpassword():
            return CORRECT_PASSWORD

        async def get_wrongpassword():
            return 'wrongpassword'

        conn = await self.connect(
            user='password_user',
            password=lambda: get_correctpassword())
        await conn.close()

        with self.assertRaisesRegex(
                asyncpg.InvalidPasswordError,
                'password authentication failed for user "password_user"'):
            await self._try_connect(
                user='password_user',
                password=lambda: get_wrongpassword())

    async def test_auth_password_md5(self):
        conn = await self.connect(
            user='md5_user', password=CORRECT_PASSWORD)
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
            user='scram_sha_256_user', password=CORRECT_PASSWORD)
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
            f"ALTER ROLE scram_sha_256_user PASSWORD E{CORRECT_PASSWORD!r};"
        await self.con.execute(alter_password)
        await self.con.execute("SET password_encryption = 'md5';")

    @unittest.mock.patch('hashlib.md5', side_effect=ValueError("no md5"))
    async def test_auth_md5_unsupported(self, _):
        with self.assertRaisesRegex(
            exceptions.InternalClientError,
            ".*no md5.*",
        ):
            await self.connect(user='md5_user', password=CORRECT_PASSWORD)


class TestGssAuthentication(BaseTestAuthentication):
    @classmethod
    def setUpClass(cls):
        try:
            from k5test.realm import K5Realm
        except ModuleNotFoundError:
            raise unittest.SkipTest('k5test not installed')

        cls.realm = K5Realm()
        cls.addClassCleanup(cls.realm.stop)
        # Setup environment before starting the cluster.
        patch = unittest.mock.patch.dict(os.environ, cls.realm.env)
        patch.start()
        cls.addClassCleanup(patch.stop)
        # Add credentials.
        cls.realm.addprinc('postgres/localhost')
        cls.realm.extract_keytab('postgres/localhost', cls.realm.keytab)

        cls.USERS = [
            (cls.realm.user_princ, 'gss', None),
            (f'wrong-{cls.realm.user_princ}', 'gss', None),
        ]
        super().setUpClass()

        cls.cluster.override_connection_spec(host='localhost')

    @classmethod
    def get_server_settings(cls):
        settings = super().get_server_settings()
        settings['krb_server_keyfile'] = f'FILE:{cls.realm.keytab}'
        return settings

    @classmethod
    def setup_cluster(cls):
        cls.cluster = cls.new_cluster(pg_cluster.TempCluster)
        cls.start_cluster(
            cls.cluster, server_settings=cls.get_server_settings())

    async def test_auth_gssapi(self):
        conn = await self.connect(user=self.realm.user_princ)
        await conn.close()

        # Service name mismatch.
        with self.assertRaisesRegex(
            exceptions.InternalClientError,
            'Server .* not found'
        ):
            await self.connect(user=self.realm.user_princ, krbsrvname='wrong')

        # Credentials mismatch.
        with self.assertRaisesRegex(
            exceptions.InvalidAuthorizationSpecificationError,
            'GSSAPI authentication failed for user'
        ):
            await self.connect(user=f'wrong-{self.realm.user_princ}')


@unittest.skipIf(_system != 'Windows', 'SSPI is only available on Windows')
class TestSspiAuthentication(BaseTestAuthentication):
    @classmethod
    def setUpClass(cls):
        cls.username = f'{os.getlogin()}@{socket.gethostname()}'
        cls.USERS = [
            (cls.username, 'sspi', None),
            (f'wrong-{cls.username}', 'sspi', None),
        ]
        super().setUpClass()

    async def test_auth_sspi(self):
        conn = await self.connect(user=self.username)
        await conn.close()

        # Credentials mismatch.
        with self.assertRaisesRegex(
            exceptions.InvalidAuthorizationSpecificationError,
            'SSPI authentication failed for user'
        ):
            await self.connect(user=f'wrong-{self.username}')


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
                'sslmode': SSLMode.prefer,
                'target_session_attrs': 'any'})
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
                'database': 'db2',
                'target_session_attrs': 'any'})
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
                'ssl': False,
                'target_session_attrs': 'any'})
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
                'sslmode': SSLMode.allow,
                'target_session_attrs': 'any'})
        },

        #postgresql://eoapi:a2Vw%3Ayk=%29CdSis%5Bfek%5DtW=%2Fo@eoapi-primary.default.svc:5432/eoapi
        {
            'name': 'dsn_bad_characters_maybe',
            'env': {
                'PGUSER': 'eoapi',
                'PGDATABASE': 'eoapi',
                'PGPASSWORD': 'a2Vw:yk=)CdSis[fek]tW=/o',
                'PGHOST': 'eoapi-primary.default.svc',
                'PGPORT': '5432',
            },

            'dsn': 'postgres://eoapi:"a2Vw:yk=)CdSis[fek]tW=/o"@eoapi-primary.default.svc:5432/eoapi',

            'result': ([('eoapi-primary.default.svc', 5432)], {
                'user': 'eoapi',
                'password': 'a2Vw:yk=)CdSis[fek]tW=/o',
                'database': 'eoapi',
                'ssl': True})
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
                'ssl': False,
                'target_session_attrs': 'any'})
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
                'sslmode': SSLMode.prefer,
                'target_session_attrs': 'any'})
        },

        {
            'name': 'dsn_only',
            'dsn': 'postgres://user3:123123@localhost:5555/abcdef',
            'result': ([('localhost', 5555)], {
                'user': 'user3',
                'password': '123123',
                'database': 'abcdef',
                'target_session_attrs': 'any'})
        },

        {
            'name': 'dsn_only_multi_host',
            'dsn': 'postgresql://user@host1,host2/db',
            'result': ([('host1', 5432), ('host2', 5432)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
            })
        },

        {
            'name': 'dsn_only_multi_host_and_port',
            'dsn': 'postgresql://user@host1:1111,host2:2222/db',
            'result': ([('host1', 1111), ('host2', 2222)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
            })
        },

        {
            'name': 'target_session_attrs',
            'dsn': 'postgresql://user@host1:1111,host2:2222/db'
                   '?target_session_attrs=read-only',
            'result': ([('host1', 1111), ('host2', 2222)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'read-only',
            })
        },

        {
            'name': 'target_session_attrs_2',
            'dsn': 'postgresql://user@host1:1111,host2:2222/db'
                   '?target_session_attrs=read-only',
            'target_session_attrs': 'read-write',
            'result': ([('host1', 1111), ('host2', 2222)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'read-write',
            })
        },

        {
            'name': 'target_session_attrs_3',
            'dsn': 'postgresql://user@host1:1111,host2:2222/db',
            'env': {
                'PGTARGETSESSIONATTRS': 'read-only',
            },
            'result': ([('host1', 1111), ('host2', 2222)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'read-only',
            })
        },

        {
            'name': 'krbsrvname',
            'dsn': 'postgresql://user@host/db?krbsrvname=srv_qs',
            'env': {
                'PGKRBSRVNAME': 'srv_env',
            },
            'result': ([('host', 5432)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
                'krbsrvname': 'srv_qs',
            })
        },

        {
            'name': 'krbsrvname_2',
            'dsn': 'postgresql://user@host/db?krbsrvname=srv_qs',
            'krbsrvname': 'srv_kws',
            'env': {
                'PGKRBSRVNAME': 'srv_env',
            },
            'result': ([('host', 5432)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
                'krbsrvname': 'srv_kws',
            })
        },

        {
            'name': 'krbsrvname_3',
            'dsn': 'postgresql://user@host/db',
            'env': {
                'PGKRBSRVNAME': 'srv_env',
            },
            'result': ([('host', 5432)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
                'krbsrvname': 'srv_env',
            })
        },

        {
            'name': 'gsslib',
            'dsn': f'postgresql://user@host/db?gsslib={OTHER_GSSLIB}',
            'env': {
                'PGGSSLIB': 'ignored',
            },
            'result': ([('host', 5432)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
                'gsslib': OTHER_GSSLIB,
            })
        },

        {
            'name': 'gsslib_2',
            'dsn': 'postgresql://user@host/db?gsslib=ignored',
            'gsslib': OTHER_GSSLIB,
            'env': {
                'PGGSSLIB': 'ignored',
            },
            'result': ([('host', 5432)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
                'gsslib': OTHER_GSSLIB,
            })
        },

        {
            'name': 'gsslib_3',
            'dsn': 'postgresql://user@host/db',
            'env': {
                'PGGSSLIB': OTHER_GSSLIB,
            },
            'result': ([('host', 5432)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
                'gsslib': OTHER_GSSLIB,
            })
        },

        {
            'name': 'gsslib_4',
            'dsn': 'postgresql://user@host/db',
            'result': ([('host', 5432)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
                'gsslib': DEFAULT_GSSLIB,
            })
        },

        {
            'name': 'gsslib_5',
            'dsn': 'postgresql://user@host/db?gsslib=invalid',
            'error': (
                exceptions.ClientConfigurationError,
                "gsslib parameter must be either 'gssapi' or 'sspi'"
            ),
        },

        {
            'name': 'dsn_ipv6_multi_host',
            'dsn': 'postgresql://user@[2001:db8::1234%25eth0],[::1]/db',
            'result': ([('2001:db8::1234%eth0', 5432), ('::1', 5432)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
            })
        },

        {
            'name': 'dsn_ipv6_multi_host_port',
            'dsn': 'postgresql://user@[2001:db8::1234]:1111,[::1]:2222/db',
            'result': ([('2001:db8::1234', 1111), ('::1', 2222)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
            })
        },

        {
            'name': 'dsn_ipv6_multi_host_query_part',
            'dsn': 'postgresql:///db?user=user&host=[2001:db8::1234],[::1]',
            'result': ([('2001:db8::1234', 5432), ('::1', 5432)], {
                'database': 'db',
                'user': 'user',
                'target_session_attrs': 'any',
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
                'target_session_attrs': 'any',
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
                'target_session_attrs': 'any',
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
                'target_session_attrs': 'any',
            })
        },
        {
            'name': 'params_multi_host_dsn_env_mix_tuple',
            'env': {
                'PGUSER': 'foo',
            },
            'dsn': 'postgresql:///db',
            'host': ('host1', 'host2'),
            'result': ([('host1', 5432), ('host2', 5432)], {
                'database': 'db',
                'user': 'foo',
                'target_session_attrs': 'any',
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
                'sslmode': SSLMode.require,
                'target_session_attrs': 'any'})
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
                'ssl': True,
                'target_session_attrs': 'any'})
        },

        {
            'name': 'dsn_only_unix',
            'dsn': 'postgresql:///dbname?host=/unix_sock/test&user=spam',
            'result': ([os.path.join('/unix_sock/test', '.s.PGSQL.5432')], {
                'user': 'spam',
                'database': 'dbname',
                'target_session_attrs': 'any'})
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
                    'target_session_attrs': 'any',
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
                    'target_session_attrs': 'any',
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
                    'target_session_attrs': 'any',
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
                    'ssl': None,
                    'target_session_attrs': 'any',
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
                    'database': 'db',
                    'target_session_attrs': 'any',
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
                    'target_session_attrs': 'any',
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
        target_session_attrs = testcase.get('target_session_attrs')
        krbsrvname = testcase.get('krbsrvname')
        gsslib = testcase.get('gsslib')

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
                direct_tls=False,
                server_settings=server_settings,
                target_session_attrs=target_session_attrs,
                krbsrvname=krbsrvname, gsslib=gsslib)

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
            if 'direct_tls' not in expected[1]:
                # Avoid the hassle of specifying direct_tls
                # unless explicitly tested for
                params.pop('direct_tls', False)
            if 'gsslib' not in expected[1]:
                # Avoid the hassle of specifying gsslib
                # unless explicitly tested for
                params.pop('gsslib', None)

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
                    {'user': '__test__',
                     'database': '__test__',
                     'target_session_attrs': 'any'}
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
                        'target_session_attrs': 'any',
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
                        'target_session_attrs': 'any',
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
                        'target_session_attrs': 'any',
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
                        'target_session_attrs': 'any',
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
                            'target_session_attrs': 'any',
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
                        'target_session_attrs': 'any',
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
                        'target_session_attrs': 'any',
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
                        'target_session_attrs': 'any',
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
                        'target_session_attrs': 'any',
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
                            'target_session_attrs': 'any',
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
                            'target_session_attrs': 'any',
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
                    'target_session_attrs': 'any',
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
                        'target_session_attrs': 'any',
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
                                'target_session_attrs': 'any',
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
        self.assertTrue(isinstance(self.con, pg_connection.Connection))
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
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
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
                    user='postgres',
                    database='postgres',
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
                        user='postgres',
                        database='postgres',
                        host='localhost')
                    await con.fetchval('SELECT 42')
            finally:
                if con:
                    await con.close()

        await verify_works('disable')
        await verify_works('allow')
        await verify_works('prefer')
        await verify_fails('require')
        with mock_dot_postgresql():
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

    @unittest.skipIf(os.environ.get('PGHOST'), 'unmanaged cluster')
    async def test_connection_no_home_dir(self):
        with mock_no_home_dir():
            con = await self.connect(
                dsn='postgresql://foo/',
                user='postgres',
                database='postgres',
                host='localhost')
            await con.fetchval('SELECT 42')
            await con.close()

        with mock_dev_null_home_dir():
            con = await self.connect(
                dsn='postgresql://foo/',
                user='postgres',
                database='postgres',
                host='localhost')
            await con.fetchval('SELECT 42')
            await con.close()

        with self.assertRaisesRegex(
            exceptions.ClientConfigurationError,
            r'root certificate file "~/\.postgresql/root\.crt" does not exist'
        ):
            with mock_no_home_dir():
                await self.connect(
                    host='localhost',
                    user='ssl_user',
                    ssl='verify-full')

        with self.assertRaisesRegex(
            exceptions.ClientConfigurationError,
            r'root certificate file ".*" does not exist'
        ):
            with mock_dev_null_home_dir():
                await self.connect(
                    host='localhost',
                    user='ssl_user',
                    ssl='verify-full')


class BaseTestSSLConnection(tb.ConnectedTestCase):
    @classmethod
    def get_server_settings(cls):
        conf = super().get_server_settings()
        conf.update({
            'ssl': 'on',
            'ssl_cert_file': SSL_CERT_FILE,
            'ssl_key_file': SSL_KEY_FILE,
            'ssl_ca_file': CLIENT_CA_CERT_FILE,
        })
        if cls.cluster.get_pg_version() >= (12, 0):
            conf['ssl_min_protocol_version'] = 'TLSv1.2'
            conf['ssl_max_protocol_version'] = 'TLSv1.2'

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
        create_script.append('GRANT ALL ON SCHEMA public TO ssl_user;')

        self._add_hba_entry()

        # Put hba changes into effect
        self.cluster.reload()

        create_script = '\n'.join(create_script)
        self.loop.run_until_complete(self.con.execute(create_script))

    def tearDown(self):
        # Reset cluster's pg_hba.conf since we've meddled with it
        self.cluster.trust_local_connections()

        drop_script = []
        drop_script.append('REVOKE ALL ON SCHEMA public FROM ssl_user;')
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
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
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
                    dsn='postgresql://foo/postgres?sslmode=' + sslmode,
                    host=host,
                    user='ssl_user')
                self.assertEqual(await con.fetchval('SELECT 42'), 42)
                self.assertTrue(con._protocol.is_ssl)
            finally:
                if con:
                    await con.close()

        async def verify_fails(sslmode, *, host='localhost', exn_type):
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
        await verify_fails('verify-ca', exn_type=ValueError)
        await verify_fails('verify-full', exn_type=ValueError)

        with mock_dot_postgresql():
            await verify_works('require')
            await verify_works('verify-ca')
            await verify_works('verify-ca', host='127.0.0.1')
            await verify_works('verify-full')
            await verify_fails('verify-full', host='127.0.0.1',
                               exn_type=ssl.CertificateError)

        with mock_dot_postgresql(crl=True):
            await verify_fails('disable', exn_type=invalid_auth_err)
            await verify_works('allow')
            await verify_works('prefer')
            await verify_fails('require',
                               exn_type=ssl.SSLError)
            await verify_fails('verify-ca',
                               exn_type=ssl.SSLError)
            await verify_fails('verify-ca', host='127.0.0.1',
                               exn_type=ssl.SSLError)
            await verify_fails('verify-full',
                               exn_type=ssl.SSLError)

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
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
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
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
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
                await con.execute('DROP TABLE IF EXISTS test_many')
            finally:
                await con.close()

    async def test_tls_version(self):
        if self.cluster.get_pg_version() < (12, 0):
            self.skipTest("PostgreSQL < 12 cannot set ssl protocol version")

        # XXX: uvloop artifact
        old_handler = self.loop.get_exception_handler()

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="ssl.TLSVersion.TLSv1_1 is deprecated",
                category=DeprecationWarning
            )
            try:
                self.loop.set_exception_handler(lambda *args: None)
                with self.assertRaisesRegex(
                    ssl.SSLError,
                    '(protocol version)|(handshake failure)',
                ):
                    await self.connect(
                        dsn='postgresql://ssl_user@localhost/postgres'
                            '?sslmode=require&ssl_min_protocol_version=TLSv1.3'
                    )
                with self.assertRaises((ssl.SSLError, ConnectionResetError)):
                    await self.connect(
                        dsn='postgresql://ssl_user@localhost/postgres'
                            '?sslmode=require'
                            '&ssl_min_protocol_version=TLSv1.1'
                            '&ssl_max_protocol_version=TLSv1.1'
                    )
                if not ssl.OPENSSL_VERSION.startswith('LibreSSL'):
                    with self.assertRaisesRegex(ssl.SSLError, 'no protocols'):
                        await self.connect(
                            dsn='postgresql://ssl_user@localhost/postgres'
                                '?sslmode=require'
                                '&ssl_min_protocol_version=TLSv1.2'
                                '&ssl_max_protocol_version=TLSv1.1'
                        )
                con = await self.connect(
                    dsn='postgresql://ssl_user@localhost/postgres'
                        '?sslmode=require'
                        '&ssl_min_protocol_version=TLSv1.2'
                        '&ssl_max_protocol_version=TLSv1.2'
                )
                try:
                    self.assertEqual(await con.fetchval('SELECT 42'), 42)
                finally:
                    await con.close()
            finally:
                self.loop.set_exception_handler(old_handler)


@unittest.skipIf(os.environ.get('PGHOST'), 'unmanaged cluster')
class TestClientSSLConnection(BaseTestSSLConnection):
    def _add_hba_entry(self):
        self.cluster.add_hba_entry(
            type='hostssl', address=ipaddress.ip_network('127.0.0.0/24'),
            database='postgres', user='ssl_user',
            auth_method='cert')

        self.cluster.add_hba_entry(
            type='hostssl', address=ipaddress.ip_network('::1/128'),
            database='postgres', user='ssl_user',
            auth_method='cert')

    async def test_ssl_connection_client_auth_fails_with_wrong_setup(self):
        ssl_context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=SSL_CA_CERT_FILE,
        )

        with self.assertRaisesRegex(
            exceptions.InvalidAuthorizationSpecificationError,
            "requires a valid client certificate",
        ):
            await self.connect(
                host='localhost',
                user='ssl_user',
                ssl=ssl_context,
            )

    async def _test_works(self, **conn_args):
        con = await self.connect(**conn_args)

        try:
            self.assertEqual(await con.fetchval('SELECT 42'), 42)
        finally:
            await con.close()

    async def test_ssl_connection_client_auth_custom_context(self):
        for key_file in (CLIENT_SSL_KEY_FILE, CLIENT_SSL_PROTECTED_KEY_FILE):
            ssl_context = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH,
                cafile=SSL_CA_CERT_FILE,
            )
            ssl_context.load_cert_chain(
                CLIENT_SSL_CERT_FILE,
                keyfile=key_file,
                password='secRet',
            )
            await self._test_works(
                host='localhost',
                user='ssl_user',
                ssl=ssl_context,
            )

    async def test_ssl_connection_client_auth_dsn(self):
        params = {
            'sslrootcert': SSL_CA_CERT_FILE,
            'sslcert': CLIENT_SSL_CERT_FILE,
            'sslkey': CLIENT_SSL_KEY_FILE,
            'sslmode': 'verify-full',
        }
        params_str = urllib.parse.urlencode(params)
        dsn = 'postgres://ssl_user@localhost/postgres?' + params_str
        await self._test_works(dsn=dsn)

        params['sslkey'] = CLIENT_SSL_PROTECTED_KEY_FILE
        params['sslpassword'] = 'secRet'
        params_str = urllib.parse.urlencode(params)
        dsn = 'postgres://ssl_user@localhost/postgres?' + params_str
        await self._test_works(dsn=dsn)

    async def test_ssl_connection_client_auth_env(self):
        env = {
            'PGSSLROOTCERT': SSL_CA_CERT_FILE,
            'PGSSLCERT': CLIENT_SSL_CERT_FILE,
            'PGSSLKEY': CLIENT_SSL_KEY_FILE,
        }
        dsn = 'postgres://ssl_user@localhost/postgres?sslmode=verify-full'
        with unittest.mock.patch.dict('os.environ', env):
            await self._test_works(dsn=dsn)

        env['PGSSLKEY'] = CLIENT_SSL_PROTECTED_KEY_FILE
        with unittest.mock.patch.dict('os.environ', env):
            await self._test_works(dsn=dsn + '&sslpassword=secRet')

    async def test_ssl_connection_client_auth_dot_postgresql(self):
        dsn = 'postgres://ssl_user@localhost/postgres?sslmode=verify-full'
        with mock_dot_postgresql(client=True):
            await self._test_works(dsn=dsn)
        with mock_dot_postgresql(client=True, protected=True):
            await self._test_works(dsn=dsn + '&sslpassword=secRet')


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
                    dsn='postgresql://foo/postgres?sslmode=' + sslmode,
                    host=host,
                    user='ssl_user')
                self.assertEqual(await con.fetchval('SELECT 42'), 42)
                self.assertFalse(con._protocol.is_ssl)
            finally:
                if con:
                    await con.close()

        async def verify_fails(sslmode, *, host='localhost'):
            # XXX: uvloop artifact
            old_handler = self.loop.get_exception_handler()
            con = None
            try:
                self.loop.set_exception_handler(lambda *args: None)
                with self.assertRaises(
                        asyncpg.InvalidAuthorizationSpecificationError
                ):
                    con = await self.connect(
                        dsn='postgresql://foo/?sslmode=' + sslmode,
                        host=host,
                        user='ssl_user')
                    await con.fetchval('SELECT 42')
            finally:
                if con:
                    await con.close()
                self.loop.set_exception_handler(old_handler)

        await verify_works('disable')
        await verify_works('allow')
        await verify_works('prefer')
        await verify_fails('require')
        with mock_dot_postgresql():
            await verify_fails('require')
            await verify_fails('verify-ca')
            await verify_fails('verify-full')

    async def test_nossl_connection_prefer_cancel(self):
        con = await self.connect(
            dsn='postgresql://foo/postgres?sslmode=prefer',
            host='localhost',
            user='ssl_user')
        try:
            self.assertFalse(con._protocol.is_ssl)
            with self.assertRaises(asyncio.TimeoutError):
                await con.execute('SELECT pg_sleep(5)', timeout=0.5)
            val = await con.fetchval('SELECT 123')
            self.assertEqual(val, 123)
        finally:
            await con.close()

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
        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            con = await self.connect()
            await con.fetchval("select 123")
            proto = con._protocol
            conref = weakref.ref(con)
            del con

            self.assertIsNone(conref())
            self.assertTrue(proto.is_closed())

            # tick event loop; asyncio.selector_events._SelectorSocketTransport
            # needs a chance to close itself and remove its reference to proto
            await asyncio.sleep(0)
            protoref = weakref.ref(proto)
            del proto
            self.assertIsNone(protoref())
        finally:
            if gc_was_enabled:
                gc.enable()

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

            msg = " ".join(rw.warning.args)
            self.assertIn(' created at:\n', msg)
            self.assertIn('in test_no_explicit_close_with_debug', msg)
        finally:
            self.loop.set_debug(olddebug)


class TestConnectionAttributes(tb.HotStandbyTestCase):

    async def _run_connection_test(
        self, connect, target_attribute, expected_port
    ):
        conn = await connect(target_session_attrs=target_attribute)
        self.assertTrue(_get_connected_host(conn).endswith(expected_port))
        await conn.close()

    async def test_target_server_attribute_port(self):
        master_port = self.master_cluster.get_connection_spec()['port']
        standby_port = self.standby_cluster.get_connection_spec()['port']
        tests = [
            (self.connect_primary, 'primary', master_port),
            (self.connect_standby, 'standby', standby_port),
        ]

        for connect, target_attr, expected_port in tests:
            await self._run_connection_test(
                connect, target_attr, expected_port
            )
        if self.master_cluster.get_pg_version()[0] < 14:
            self.skipTest("PostgreSQL<14 does not support these features")
        tests = [
            (self.connect_primary, 'read-write', master_port),
            (self.connect_standby, 'read-only', standby_port),
        ]

        for connect, target_attr, expected_port in tests:
            await self._run_connection_test(
                connect, target_attr, expected_port
            )

    async def test_target_attribute_not_matched(self):
        tests = [
            (self.connect_standby, 'primary'),
            (self.connect_primary, 'standby'),
        ]

        for connect, target_attr in tests:
            with self.assertRaises(exceptions.TargetServerAttributeNotMatched):
                await connect(target_session_attrs=target_attr)

        if self.master_cluster.get_pg_version()[0] < 14:
            self.skipTest("PostgreSQL<14 does not support these features")
        tests = [
            (self.connect_standby, 'read-write'),
            (self.connect_primary, 'read-only'),
        ]

        for connect, target_attr in tests:
            with self.assertRaises(exceptions.TargetServerAttributeNotMatched):
                await connect(target_session_attrs=target_attr)

    async def test_prefer_standby_when_standby_is_up(self):
        con = await self.connect(target_session_attrs='prefer-standby')
        standby_port = self.standby_cluster.get_connection_spec()['port']
        connected_host = _get_connected_host(con)
        self.assertTrue(connected_host.endswith(standby_port))
        await con.close()

    async def test_prefer_standby_picks_master_when_standby_is_down(self):
        primary_spec = self.get_cluster_connection_spec(self.master_cluster)
        connection_spec = {
            'host': [
                primary_spec['host'],
                'unlocalhost',
            ],
            'port': [primary_spec['port'], 15345],
            'database': primary_spec['database'],
            'user': primary_spec['user'],
            'target_session_attrs': 'prefer-standby'
        }

        con = await self.connect(**connection_spec)
        master_port = self.master_cluster.get_connection_spec()['port']
        connected_host = _get_connected_host(con)
        self.assertTrue(connected_host.endswith(master_port))
        await con.close()


def _get_connected_host(con):
    peername = con._transport.get_extra_info('peername')
    if isinstance(peername, tuple):
        peername = "".join((str(s) for s in peername if s))
    return peername
