# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import asyncio
import enum
import functools
import getpass
import os
import pathlib
import platform
import random
import re
import socket
import ssl as ssl_module
import stat
import struct
import sys
import typing
import urllib.parse
import warnings
import inspect

from . import compat
from . import exceptions
from . import protocol

if typing.TYPE_CHECKING:
    if sys.version_info < (3, 11):
        from typing_extensions import Self
    else:
        from typing import Self

    from . import connection

_ConnectionT = typing.TypeVar(
    '_ConnectionT',
    bound='connection.Connection[typing.Any]'
)
_ProtocolT = typing.TypeVar(
    '_ProtocolT',
    bound='protocol.Protocol[typing.Any]'
)
_AsyncProtocolT = typing.TypeVar(
    '_AsyncProtocolT', bound='asyncio.protocols.Protocol'
)
_RecordT = typing.TypeVar('_RecordT', bound=protocol.Record)
_ParsedSSLType = typing.Union[
    ssl_module.SSLContext, typing.Literal[False]
]
_SSLStringValues = typing.Literal[
    'disable', 'prefer', 'allow', 'require', 'verify-ca', 'verify-full'
]
_TPTupleType = compat.tuple[
    asyncio.WriteTransport,
    _AsyncProtocolT
]
AddrType = typing.Union[
    compat.tuple[str, int],
    str
]
HostType = typing.Union[compat.list[str], compat.tuple[str, ...], str]
PasswordType = typing.Union[
    str,
    compat.Callable[[], str],
    compat.Callable[[], compat.Awaitable[str]]
]
PortListType = typing.Union[
    compat.list[typing.Union[int, str]],
    compat.list[int],
    compat.list[str],
]
PortType = typing.Union[
    PortListType,
    int,
    str
]
SSLType = typing.Union[_ParsedSSLType, _SSLStringValues, bool]


class SSLMode(enum.IntEnum):
    disable = 0
    allow = 1
    prefer = 2
    require = 3
    verify_ca = 4
    verify_full = 5

    @classmethod
    def parse(cls, sslmode: str | Self) -> Self:
        if isinstance(sslmode, cls):
            return sslmode
        return typing.cast(
            'Self',
            getattr(cls, typing.cast(str, sslmode).replace('-', '_'))
        )


class _ConnectionParameters(typing.NamedTuple):
    user: str
    password: PasswordType | None
    database: str
    ssl: _ParsedSSLType | None
    sslmode: SSLMode | None
    direct_tls: bool
    server_settings: dict[str, str] | None
    target_session_attrs: SessionAttribute


class _ClientConfiguration(typing.NamedTuple):
    command_timeout: float | None
    statement_cache_size: int
    max_cached_statement_lifetime: int
    max_cacheable_statement_size: int


_system: typing.Final = platform.uname().system
PGPASSFILE: typing.Final = (
    'pgpass.conf' if _system == 'Windows' else '.pgpass'
)


def _read_password_file(passfile: pathlib.Path) -> list[tuple[str, ...]]:
    passtab = []

    try:
        if not passfile.exists():
            return []

        if not passfile.is_file():
            warnings.warn(
                'password file {!r} is not a plain file'.format(passfile))

            return []

        if _system != 'Windows':
            if passfile.stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO):
                warnings.warn(
                    'password file {!r} has group or world access; '
                    'permissions should be u=rw (0600) or less'.format(
                        passfile))

                return []

        with passfile.open('rt') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    # Skip empty lines and comments.
                    continue
                # Backslash escapes both itself and the colon,
                # which is a record separator.
                line = line.replace(R'\\', '\n')
                passtab.append(tuple(
                    p.replace('\n', R'\\')
                    for p in re.split(r'(?<!\\):', line, maxsplit=4)
                ))
    except IOError:
        pass

    return passtab


def _read_password_from_pgpass(
    *,
    passfile: pathlib.Path,
    hosts: compat.Iterable[str],
    ports: list[int],
    database: str,
    user: str
) -> str | None:
    """Parse the pgpass file and return the matching password.

    :return:
        Password string, if found, ``None`` otherwise.
    """

    passtab = _read_password_file(passfile)
    if not passtab:
        return None

    for host, port in zip(hosts, ports):
        if host.startswith('/'):
            # Unix sockets get normalized into 'localhost'
            host = 'localhost'

        for phost, pport, pdatabase, puser, ppassword in passtab:
            if phost != '*' and phost != host:
                continue
            if pport != '*' and pport != str(port):
                continue
            if pdatabase != '*' and pdatabase != database:
                continue
            if puser != '*' and puser != user:
                continue

            # Found a match.
            return ppassword

    return None


def _validate_port_spec(hosts: compat.Sized, port: PortType) -> list[int]:
    if isinstance(port, list):
        # If there is a list of ports, its length must
        # match that of the host list.
        if len(port) != len(hosts):
            raise exceptions.ClientConfigurationError(
                'could not match {} port numbers to {} hosts'.format(
                    len(port), len(hosts)))
        return [int(p) for p in port]
    else:
        return [int(port) for _ in range(len(hosts))]


def _parse_hostlist(
    hostlist: str,
    port: PortType | None,
    *,
    unquote: bool = False
) -> tuple[list[str], PortListType]:
    if ',' in hostlist:
        # A comma-separated list of host addresses.
        hostspecs = hostlist.split(',')
    else:
        hostspecs = [hostlist]

    hosts: list[str] = []
    hostlist_ports: list[int] = []
    ports: list[int] | None = None

    if not port:
        portspec = os.environ.get('PGPORT')
        if portspec:
            if ',' in portspec:
                temp_port: list[int] | int = [
                    int(p) for p in portspec.split(',')
                ]
            else:
                temp_port = int(portspec)
        else:
            temp_port = 5432

        default_port = _validate_port_spec(hostspecs, temp_port)

    else:
        ports = _validate_port_spec(hostspecs, port)

    for i, hostspec in enumerate(hostspecs):
        if hostspec[0] == '/':
            # Unix socket
            addr = hostspec
            hostspec_port: str = ''
        elif hostspec[0] == '[':
            # IPv6 address
            m = re.match(r'(?:\[([^\]]+)\])(?::([0-9]+))?', hostspec)
            if m:
                addr = m.group(1)
                hostspec_port = m.group(2)
            else:
                raise exceptions.ClientConfigurationError(
                    'invalid IPv6 address in the connection URI: {!r}'.format(
                        hostspec
                    )
                )
        else:
            # IPv4 address
            addr, _, hostspec_port = hostspec.partition(':')

        if unquote:
            addr = urllib.parse.unquote(addr)

        hosts.append(addr)
        if not port:
            if hostspec_port:
                if unquote:
                    hostspec_port = urllib.parse.unquote(hostspec_port)
                hostlist_ports.append(int(hostspec_port))
            else:
                hostlist_ports.append(default_port[i])

    if not ports:
        ports = hostlist_ports

    return hosts, ports


def _parse_tls_version(tls_version: str) -> ssl_module.TLSVersion:
    if tls_version.startswith('SSL'):
        raise exceptions.ClientConfigurationError(
            f"Unsupported TLS version: {tls_version}"
        )
    try:
        return ssl_module.TLSVersion[tls_version.replace('.', '_')]
    except KeyError:
        raise exceptions.ClientConfigurationError(
            f"No such TLS version: {tls_version}"
        )


def _dot_postgresql_path(filename: str) -> pathlib.Path | None:
    try:
        homedir = pathlib.Path.home()
    except (RuntimeError, KeyError):
        return None

    return (homedir / '.postgresql' / filename).resolve()


def _parse_connect_dsn_and_args(
    *,
    dsn: str | None,
    host: HostType | None,
    port: PortType | None,
    user: str | None,
    password: str | None,
    passfile: str | None,
    database: str | None,
    ssl: SSLType | None,
    direct_tls: bool,
    server_settings: dict[str, str] | None,
    target_session_attrs: SessionAttribute | None,
) -> tuple[list[tuple[str, int] | str], _ConnectionParameters]:
    # `auth_hosts` is the version of host information for the purposes
    # of reading the pgpass file.
    auth_hosts: list[str] | tuple[str, ...] | None = None
    sslcert: str | pathlib.Path | None = None
    sslkey: str | pathlib.Path | None = None
    sslrootcert: str | pathlib.Path | None = None
    sslcrl: str | pathlib.Path | None = None
    sslpassword = None
    ssl_min_protocol_version = ssl_max_protocol_version = None
    ssl_val: SSLType | str | None = ssl
    ssl_parsed: _ParsedSSLType | None = None
    target_session_attrs_val: (
        SessionAttribute | str | None
    ) = target_session_attrs

    if dsn:
        parsed = urllib.parse.urlparse(dsn)

        if parsed.scheme not in {'postgresql', 'postgres'}:
            raise exceptions.ClientConfigurationError(
                'invalid DSN: scheme is expected to be either '
                '"postgresql" or "postgres", got {!r}'.format(parsed.scheme))

        if parsed.netloc:
            if '@' in parsed.netloc:
                dsn_auth, _, dsn_hostspec = parsed.netloc.partition('@')
            else:
                dsn_hostspec = parsed.netloc
                dsn_auth = ''
        else:
            dsn_auth = dsn_hostspec = ''

        if dsn_auth:
            dsn_user, _, dsn_password = dsn_auth.partition(':')
        else:
            dsn_user = dsn_password = ''

        if not host and dsn_hostspec:
            host, port = _parse_hostlist(dsn_hostspec, port, unquote=True)

        if parsed.path and database is None:
            dsn_database = parsed.path
            if dsn_database.startswith('/'):
                dsn_database = dsn_database[1:]
            database = urllib.parse.unquote(dsn_database)

        if user is None and dsn_user:
            user = urllib.parse.unquote(dsn_user)

        if password is None and dsn_password:
            password = urllib.parse.unquote(dsn_password)

        if parsed.query:
            query: dict[str, str] = {
                key: val[-1] if isinstance(val, list) else val
                for key, val in urllib.parse.parse_qs(
                    parsed.query, strict_parsing=True
                ).items()
            }

            if 'port' in query:
                val = query.pop('port')
                if not port and val:
                    port = [int(p) for p in val.split(',')]

            if 'host' in query:
                val = query.pop('host')
                if not host and val:
                    host, port = _parse_hostlist(val, port)

            if 'dbname' in query:
                val = query.pop('dbname')
                if database is None:
                    database = val

            if 'database' in query:
                val = query.pop('database')
                if database is None:
                    database = val

            if 'user' in query:
                val = query.pop('user')
                if user is None:
                    user = val

            if 'password' in query:
                val = query.pop('password')
                if password is None:
                    password = val

            if 'passfile' in query:
                val = query.pop('passfile')
                if passfile is None:
                    passfile = val

            if 'sslmode' in query:
                val = query.pop('sslmode')
                if ssl_val is None:
                    ssl_val = val

            if 'sslcert' in query:
                sslcert = query.pop('sslcert')

            if 'sslkey' in query:
                sslkey = query.pop('sslkey')

            if 'sslrootcert' in query:
                sslrootcert = query.pop('sslrootcert')

            if 'sslcrl' in query:
                sslcrl = query.pop('sslcrl')

            if 'sslpassword' in query:
                sslpassword = query.pop('sslpassword')

            if 'ssl_min_protocol_version' in query:
                ssl_min_protocol_version = query.pop(
                    'ssl_min_protocol_version'
                )

            if 'ssl_max_protocol_version' in query:
                ssl_max_protocol_version = query.pop(
                    'ssl_max_protocol_version'
                )

            if 'target_session_attrs' in query:
                dsn_target_session_attrs = query.pop(
                    'target_session_attrs'
                )
                if target_session_attrs_val is None:
                    target_session_attrs_val = dsn_target_session_attrs

            if query:
                if server_settings is None:
                    server_settings = query
                else:
                    server_settings = {**query, **server_settings}

    if not host:
        hostspec = os.environ.get('PGHOST')
        if hostspec:
            host, port = _parse_hostlist(hostspec, port)

    if not host:
        auth_hosts = ['localhost']

        if _system == 'Windows':
            host = ['localhost']
        else:
            host = ['/run/postgresql', '/var/run/postgresql',
                    '/tmp', '/private/tmp', 'localhost']

    if not isinstance(host, (list, tuple)):
        host = [host]

    if auth_hosts is None:
        auth_hosts = host

    if not port:
        portspec = os.environ.get('PGPORT')
        if portspec:
            if ',' in portspec:
                port = [int(p) for p in portspec.split(',')]
            else:
                port = int(portspec)
        else:
            port = 5432

    elif isinstance(port, (list, tuple)):
        port = [int(p) for p in port]

    else:
        port = int(port)

    validated_ports = _validate_port_spec(host, port)

    if user is None:
        user = os.getenv('PGUSER')
        if not user:
            user = getpass.getuser()

    if password is None:
        password = os.getenv('PGPASSWORD')

    if database is None:
        database = os.getenv('PGDATABASE')

    if database is None:
        database = user

    if user is None:
        raise exceptions.ClientConfigurationError(
            'could not determine user name to connect with')

    if database is None:
        raise exceptions.ClientConfigurationError(
            'could not determine database name to connect to')

    if password is None:
        if passfile is None:
            passfile = os.getenv('PGPASSFILE')

        if passfile is None:
            homedir = compat.get_pg_home_directory()
            if homedir:
                passfile_path: pathlib.Path | None = homedir / PGPASSFILE
            else:
                passfile_path = None
        else:
            passfile_path = pathlib.Path(passfile)

        if passfile_path is not None:
            password = _read_password_from_pgpass(
                hosts=auth_hosts, ports=validated_ports,
                database=database, user=user,
                passfile=passfile_path)

    addrs: list[AddrType] = []
    have_tcp_addrs = False
    for h, p in zip(host, validated_ports):
        if h.startswith('/'):
            # UNIX socket name
            if '.s.PGSQL.' not in h:
                h = os.path.join(h, '.s.PGSQL.{}'.format(p))
            addrs.append(h)
        else:
            # TCP host/port
            addrs.append((h, p))
            have_tcp_addrs = True

    if not addrs:
        raise exceptions.InternalClientError(
            'could not determine the database address to connect to')

    if ssl_val is None:
        ssl_val = os.getenv('PGSSLMODE')

    if ssl_val is None and have_tcp_addrs:
        ssl_val = 'prefer'

    if isinstance(ssl_val, (str, SSLMode)):
        try:
            sslmode = SSLMode.parse(ssl_val)
        except AttributeError:
            modes = ', '.join(m.name.replace('_', '-') for m in SSLMode)
            raise exceptions.ClientConfigurationError(
                '`sslmode` parameter must be one of: {}'.format(modes))

        # docs at https://www.postgresql.org/docs/10/static/libpq-connect.html
        if sslmode < SSLMode.allow:
            ssl_parsed = False
        else:
            ssl_parsed = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS_CLIENT)
            ssl_parsed.check_hostname = sslmode >= SSLMode.verify_full
            if sslmode < SSLMode.require:
                ssl_parsed.verify_mode = ssl_module.CERT_NONE
            else:
                if sslrootcert is None:
                    sslrootcert = os.getenv('PGSSLROOTCERT')
                if sslrootcert:
                    ssl_parsed.load_verify_locations(cafile=sslrootcert)
                    ssl_parsed.verify_mode = ssl_module.CERT_REQUIRED
                else:
                    try:
                        sslrootcert = _dot_postgresql_path('root.crt')
                        if sslrootcert is not None:
                            ssl_parsed.load_verify_locations(
                                cafile=sslrootcert
                            )
                        else:
                            raise exceptions.ClientConfigurationError(
                                'cannot determine location of user '
                                'PostgreSQL configuration directory'
                            )
                    except (
                        exceptions.ClientConfigurationError,
                        FileNotFoundError,
                        NotADirectoryError,
                    ):
                        if sslmode > SSLMode.require:
                            if sslrootcert is None:
                                sslrootcert = '~/.postgresql/root.crt'
                                detail = (
                                    'Could not determine location of user '
                                    'home directory (HOME is either unset, '
                                    'inaccessible, or does not point to a '
                                    'valid directory)'
                                )
                            else:
                                detail = None
                            raise exceptions.ClientConfigurationError(
                                f'root certificate file "{sslrootcert}" does '
                                f'not exist or cannot be accessed',
                                hint='Provide the certificate file directly '
                                     f'or make sure "{sslrootcert}" '
                                     'exists and is readable.',
                                detail=detail,
                            )
                        elif sslmode == SSLMode.require:
                            ssl_parsed.verify_mode = ssl_module.CERT_NONE
                        else:
                            assert False, 'unreachable'
                    else:
                        ssl_parsed.verify_mode = ssl_module.CERT_REQUIRED

                if sslcrl is None:
                    sslcrl = os.getenv('PGSSLCRL')
                if sslcrl:
                    ssl_parsed.load_verify_locations(cafile=sslcrl)
                    ssl_parsed.verify_flags |= (
                        ssl_module.VERIFY_CRL_CHECK_CHAIN
                    )
                else:
                    sslcrl = _dot_postgresql_path('root.crl')
                    if sslcrl is not None:
                        try:
                            ssl_parsed.load_verify_locations(cafile=sslcrl)
                        except (
                            FileNotFoundError,
                            NotADirectoryError,
                        ):
                            pass
                        else:
                            ssl_parsed.verify_flags |= \
                                ssl_module.VERIFY_CRL_CHECK_CHAIN

            if sslkey is None:
                sslkey = os.getenv('PGSSLKEY')
            if not sslkey:
                sslkey = _dot_postgresql_path('postgresql.key')
                if sslkey is not None and not sslkey.exists():
                    sslkey = None
            if not sslpassword:
                sslpassword = ''
            if sslcert is None:
                sslcert = os.getenv('PGSSLCERT')
            if sslcert:
                ssl_parsed.load_cert_chain(
                    sslcert, keyfile=sslkey, password=lambda: sslpassword
                )
            else:
                sslcert = _dot_postgresql_path('postgresql.crt')
                if sslcert is not None:
                    try:
                        ssl_parsed.load_cert_chain(
                            sslcert,
                            keyfile=sslkey,
                            password=lambda: sslpassword
                        )
                    except (FileNotFoundError, NotADirectoryError):
                        pass

            # OpenSSL 1.1.1 keylog file, copied from create_default_context()
            if hasattr(ssl, 'keylog_filename'):
                keylogfile = os.environ.get('SSLKEYLOGFILE')
                if keylogfile and not sys.flags.ignore_environment:
                    ssl_parsed.keylog_filename = keylogfile

            if ssl_min_protocol_version is None:
                ssl_min_protocol_version = os.getenv('PGSSLMINPROTOCOLVERSION')
            if ssl_min_protocol_version:
                ssl_parsed.minimum_version = _parse_tls_version(
                    ssl_min_protocol_version
                )
            else:
                ssl_parsed.minimum_version = _parse_tls_version('TLSv1.2')

            if ssl_max_protocol_version is None:
                ssl_max_protocol_version = os.getenv('PGSSLMAXPROTOCOLVERSION')
            if ssl_max_protocol_version:
                ssl_parsed.maximum_version = _parse_tls_version(
                    ssl_max_protocol_version
                )

    elif ssl_val is True:
        ssl_parsed = ssl_module.create_default_context()
        sslmode = SSLMode.verify_full
    else:
        ssl_parsed = ssl_val
        sslmode = SSLMode.disable

    if server_settings is not None and (
            not isinstance(server_settings, dict) or
            not all(isinstance(k, str) for k in server_settings) or
            not all(isinstance(v, str) for v in server_settings.values())):
        raise exceptions.ClientConfigurationError(
            'server_settings is expected to be None or '
            'a Dict[str, str]')

    if target_session_attrs_val is None:
        target_session_attrs_val = os.getenv(
            "PGTARGETSESSIONATTRS", SessionAttribute.any
        )
    try:
        target_session_attrs = SessionAttribute(target_session_attrs_val)
    except ValueError:
        raise exceptions.ClientConfigurationError(
            "target_session_attrs is expected to be one of "
            "{!r}"
            ", got {!r}".format(
                SessionAttribute.__members__.values, target_session_attrs_val
            )
        ) from None

    params = _ConnectionParameters(
        user=user, password=password, database=database, ssl=ssl_parsed,
        sslmode=sslmode, direct_tls=direct_tls,
        server_settings=server_settings,
        target_session_attrs=target_session_attrs)

    return addrs, params


def _parse_connect_arguments(
    *,
    dsn: str | None,
    host: HostType | None,
    port: PortType | None,
    user: str | None,
    password: str | None,
    passfile: str | None,
    database: str | None,
    command_timeout: float | typing.SupportsFloat | None,
    statement_cache_size: int,
    max_cached_statement_lifetime: int,
    max_cacheable_statement_size: int,
    ssl: SSLType | None,
    direct_tls: bool,
    server_settings: dict[str, str] | None,
    target_session_attrs: SessionAttribute,
) -> tuple[
    list[tuple[str, int] | str], _ConnectionParameters, _ClientConfiguration
]:
    local_vars = locals()
    for var_name in {'max_cacheable_statement_size',
                     'max_cached_statement_lifetime',
                     'statement_cache_size'}:
        var_val = local_vars[var_name]
        if var_val is None or isinstance(var_val, bool) or var_val < 0:
            raise ValueError(
                '{} is expected to be greater '
                'or equal to 0, got {!r}'.format(var_name, var_val))

    if command_timeout is not None:
        try:
            if isinstance(command_timeout, bool):
                raise ValueError
            command_timeout = float(command_timeout)
            if command_timeout <= 0:
                raise ValueError
        except ValueError:
            raise ValueError(
                'invalid command_timeout value: '
                'expected greater than 0 float (got {!r})'.format(
                    command_timeout)) from None

    addrs, params = _parse_connect_dsn_and_args(
        dsn=dsn, host=host, port=port, user=user,
        password=password, passfile=passfile, ssl=ssl,
        direct_tls=direct_tls, database=database,
        server_settings=server_settings,
        target_session_attrs=target_session_attrs)

    config = _ClientConfiguration(
        command_timeout=command_timeout,
        statement_cache_size=statement_cache_size,
        max_cached_statement_lifetime=max_cached_statement_lifetime,
        max_cacheable_statement_size=max_cacheable_statement_size,)

    return addrs, params, config


class TLSUpgradeProto(asyncio.Protocol):
    on_data: asyncio.Future[bool]
    host: str
    port: int
    ssl_context: ssl_module.SSLContext
    ssl_is_advisory: bool | None

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop | None,
        host: str,
        port: int,
        ssl_context: ssl_module.SSLContext,
        ssl_is_advisory: bool | None
    ) -> None:
        self.on_data = _create_future(loop)
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.ssl_is_advisory = ssl_is_advisory

    def data_received(self, data: bytes) -> None:
        if data == b'S':
            self.on_data.set_result(True)
        elif (self.ssl_is_advisory and
                self.ssl_context.verify_mode == ssl_module.CERT_NONE and
                data == b'N'):
            # ssl_is_advisory will imply that ssl.verify_mode == CERT_NONE,
            # since the only way to get ssl_is_advisory is from
            # sslmode=prefer. But be extra sure to disallow insecure
            # connections when the ssl context asks for real security.
            self.on_data.set_result(False)
        else:
            self.on_data.set_exception(
                ConnectionError(
                    'PostgreSQL server at "{host}:{port}" '
                    'rejected SSL upgrade'.format(
                        host=self.host, port=self.port)))

    def connection_lost(self, exc: Exception | None) -> None:
        if not self.on_data.done():
            if exc is None:
                exc = ConnectionError('unexpected connection_lost() call')
            self.on_data.set_exception(exc)


@typing.overload
async def _create_ssl_connection(
    protocol_factory: compat.Callable[[], _ProtocolT],
    host: str,
    port: int,
    *,
    loop: asyncio.AbstractEventLoop,
    ssl_context: ssl_module.SSLContext,
    ssl_is_advisory: bool | None = False
) -> _TPTupleType[_ProtocolT]:
    ...


@typing.overload
async def _create_ssl_connection(
    protocol_factory: compat.Callable[[], '_CancelProto'],
    host: str,
    port: int,
    *,
    loop: asyncio.AbstractEventLoop,
    ssl_context: ssl_module.SSLContext,
    ssl_is_advisory: bool | None = False
) -> _TPTupleType['_CancelProto']:
    ...


async def _create_ssl_connection(
    protocol_factory: compat.Callable[
        [], _ProtocolT
    ] | compat.Callable[
        [], '_CancelProto'
    ],
    host: str,
    port: int,
    *,
    loop: asyncio.AbstractEventLoop,
    ssl_context: ssl_module.SSLContext,
    ssl_is_advisory: typing.Optional[bool] = False
) -> _TPTupleType[typing.Any]:

    tr, pr = typing.cast(
        compat.tuple[asyncio.WriteTransport, TLSUpgradeProto],
        await loop.create_connection(
            lambda: TLSUpgradeProto(
                loop, host, port, ssl_context, ssl_is_advisory
            ),
            host,
            port
        )
    )

    tr.write(struct.pack('!ll', 8, 80877103))  # SSLRequest message.

    try:
        do_ssl_upgrade = await pr.on_data
    except (Exception, asyncio.CancelledError):
        tr.close()
        raise

    if hasattr(loop, 'start_tls'):
        if do_ssl_upgrade:
            try:
                new_tr = typing.cast(
                    asyncio.WriteTransport,
                    await loop.start_tls(
                        tr, pr, ssl_context, server_hostname=host
                    )
                )
            except (Exception, asyncio.CancelledError):
                tr.close()
                raise
        else:
            new_tr = tr

        pg_proto = protocol_factory()
        pg_proto.is_ssl = do_ssl_upgrade
        pg_proto.connection_made(new_tr)
        new_tr.set_protocol(pg_proto)

        return new_tr, pg_proto
    else:
        conn_factory = functools.partial(
            loop.create_connection, protocol_factory)

        if do_ssl_upgrade:
            conn_factory = functools.partial(
                conn_factory, ssl=ssl_context, server_hostname=host)

        sock = _get_socket(tr)
        sock = sock.dup()
        _set_nodelay(sock)
        tr.close()

        try:
            new_tr, pg_proto = await conn_factory(sock=sock)
            pg_proto.is_ssl = do_ssl_upgrade
            return new_tr, pg_proto
        except (Exception, asyncio.CancelledError):
            sock.close()
            raise


async def _connect_addr(
    *,
    addr: AddrType,
    loop: asyncio.AbstractEventLoop,
    params: _ConnectionParameters,
    config: _ClientConfiguration,
    connection_class: type[_ConnectionT],
    record_class: type[_RecordT]
) -> _ConnectionT:
    assert loop is not None

    params_input = params
    if callable(params.password):
        password = params.password()
        if inspect.isawaitable(password):
            password = await password

        params = params._replace(password=typing.cast(str, password))
    args = (addr, loop, config, connection_class, record_class, params_input)

    # prepare the params (which attempt has ssl) for the 2 attempts
    if params.sslmode == SSLMode.allow:
        params_retry = params
        params = params._replace(ssl=None)
    elif params.sslmode == SSLMode.prefer:
        params_retry = params._replace(ssl=None)
    else:
        # skip retry if we don't have to
        return await __connect_addr(params, False, *args)

    # first attempt
    try:
        return await __connect_addr(params, True, *args)
    except _RetryConnectSignal:
        pass

    # second attempt
    return await __connect_addr(params_retry, False, *args)


class _RetryConnectSignal(Exception):
    pass


async def __connect_addr(
    params: _ConnectionParameters,
    retry: bool,
    addr: AddrType,
    loop: asyncio.AbstractEventLoop,
    config: _ClientConfiguration,
    connection_class: type[_ConnectionT],
    record_class: type[_RecordT],
    params_input: _ConnectionParameters,
) -> _ConnectionT:
    connected = _create_future(loop)

    proto_factory = lambda: protocol.Protocol(
        addr, connected, params, record_class, loop)

    if isinstance(addr, str):
        # UNIX socket
        connector = typing.cast(
            compat.Coroutine[
                typing.Any, None, _TPTupleType['protocol.Protocol[_RecordT]']
            ],
            loop.create_unix_connection(proto_factory, addr)
        )

    elif params.ssl and params.direct_tls:
        # if ssl and direct_tls are given, skip STARTTLS and perform direct
        # SSL connection
        connector = typing.cast(
            compat.Coroutine[
                typing.Any, None, _TPTupleType['protocol.Protocol[_RecordT]']
            ],
            loop.create_connection(proto_factory, *addr, ssl=params.ssl)
        )

    elif params.ssl:
        connector = _create_ssl_connection(
            proto_factory, *addr, loop=loop, ssl_context=params.ssl,
            ssl_is_advisory=params.sslmode == SSLMode.prefer)
    else:
        connector = typing.cast(
            compat.Coroutine[
                typing.Any, None, _TPTupleType['protocol.Protocol[_RecordT]']
            ],
            loop.create_connection(proto_factory, *addr)
        )

    tr, pr = await connector

    try:
        await connected
    except (
        exceptions.InvalidAuthorizationSpecificationError,
        exceptions.ConnectionDoesNotExistError,  # seen on Windows
    ):
        tr.close()

        # retry=True here is a redundant check because we don't want to
        # accidentally raise the internal _RetryConnectSignal to the user
        if retry and (
            params.sslmode == SSLMode.allow and not pr.is_ssl or
            params.sslmode == SSLMode.prefer and pr.is_ssl
        ):
            # Trigger retry when:
            #   1. First attempt with sslmode=allow, ssl=None failed
            #   2. First attempt with sslmode=prefer, ssl=ctx failed while the
            #      server claimed to support SSL (returning "S" for SSLRequest)
            #      (likely because pg_hba.conf rejected the connection)
            raise _RetryConnectSignal()

        else:
            # but will NOT retry if:
            #   1. First attempt with sslmode=prefer failed but the server
            #      doesn't support SSL (returning 'N' for SSLRequest), because
            #      we already tried to connect without SSL thru ssl_is_advisory
            #   2. Second attempt with sslmode=prefer, ssl=None failed
            #   3. Second attempt with sslmode=allow, ssl=ctx failed
            #   4. Any other sslmode
            raise

    except (Exception, asyncio.CancelledError):
        tr.close()
        raise

    con = connection_class(pr, tr, loop, addr, config, params_input)
    pr.set_connection(con)
    return con


class SessionAttribute(str, enum.Enum):
    any = 'any'
    primary = 'primary'
    standby = 'standby'
    prefer_standby = 'prefer-standby'
    read_write = "read-write"
    read_only = "read-only"


def _accept_in_hot_standby(should_be_in_hot_standby: bool) -> compat.Callable[
    [connection.Connection[typing.Any]], compat.Awaitable[bool]
]:
    """
    If the server didn't report "in_hot_standby" at startup, we must determine
    the state by checking "SELECT pg_catalog.pg_is_in_recovery()".
    If the server allows a connection and states it is in recovery it must
    be a replica/standby server.
    """
    async def can_be_used(
        connection: connection.Connection[typing.Any]
    ) -> bool:
        settings = connection.get_settings()
        hot_standby_status: str | None = getattr(
            settings, 'in_hot_standby', None
        )
        if hot_standby_status is not None:
            is_in_hot_standby: bool = hot_standby_status == 'on'
        else:
            is_in_hot_standby = await connection.fetchval(
                "SELECT pg_catalog.pg_is_in_recovery()"
            )
        return is_in_hot_standby == should_be_in_hot_standby

    return can_be_used


def _accept_read_only(should_be_read_only: bool) -> compat.Callable[
    [connection.Connection[typing.Any]], compat.Awaitable[bool]
]:
    """
    Verify the server has not set default_transaction_read_only=True
    """
    async def can_be_used(
        connection: connection.Connection[typing.Any]
    ) -> bool:
        settings = connection.get_settings()
        is_readonly = getattr(settings, 'default_transaction_read_only', 'off')

        if is_readonly == "on":
            return should_be_read_only

        return await _accept_in_hot_standby(should_be_read_only)(connection)
    return can_be_used


async def _accept_any(_: connection.Connection[typing.Any]) -> bool:
    return True


target_attrs_check: typing.Final[
    dict[
        SessionAttribute,
        compat.Callable[
            [connection.Connection[typing.Any]],
            compat.Awaitable[bool]
        ]
    ]
] = {
    SessionAttribute.any: _accept_any,
    SessionAttribute.primary: _accept_in_hot_standby(False),
    SessionAttribute.standby: _accept_in_hot_standby(True),
    SessionAttribute.prefer_standby: _accept_in_hot_standby(True),
    SessionAttribute.read_write: _accept_read_only(False),
    SessionAttribute.read_only: _accept_read_only(True),
}


async def _can_use_connection(
    connection: connection.Connection[typing.Any],
    attr: SessionAttribute
) -> bool:
    can_use = target_attrs_check[attr]
    return await can_use(connection)


async def _connect(
    *,
    loop: asyncio.AbstractEventLoop | None,
    connection_class: type[_ConnectionT],
    record_class: type[_RecordT],
    **kwargs: typing.Any
) -> _ConnectionT:
    if loop is None:
        loop = asyncio.get_event_loop()

    addrs, params, config = _parse_connect_arguments(**kwargs)
    target_attr = params.target_session_attrs

    candidates: list[_ConnectionT] = []
    chosen_connection = None
    last_error: BaseException | None = None
    for addr in addrs:
        try:
            conn = await _connect_addr(
                addr=addr,
                loop=loop,
                params=params,
                config=config,
                connection_class=connection_class,
                record_class=record_class,
            )
            candidates.append(conn)
            if await _can_use_connection(conn, target_attr):
                chosen_connection = conn
                break
        except OSError as ex:
            last_error = ex
    else:
        if target_attr == SessionAttribute.prefer_standby and candidates:
            chosen_connection = random.choice(candidates)

    await asyncio.gather(
        *(c.close() for c in candidates if c is not chosen_connection),
        return_exceptions=True
    )

    if chosen_connection:
        return chosen_connection

    raise last_error or exceptions.TargetServerAttributeNotMatched(
        'None of the hosts match the target attribute requirement '
        '{!r}'.format(target_attr)
    )


class _CancelProto(asyncio.Protocol):

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.on_disconnect = _create_future(loop)
        self.is_ssl = False

    def connection_lost(self, exc: Exception | None) -> None:
        if not self.on_disconnect.done():
            self.on_disconnect.set_result(True)


async def _cancel(
    *,
    loop: asyncio.AbstractEventLoop,
    addr: AddrType,
    params: _ConnectionParameters,
    backend_pid: int,
    backend_secret: str
) -> None:
    proto_factory: compat.Callable[
        [], _CancelProto
    ] = lambda: _CancelProto(loop)

    if isinstance(addr, str):
        tr, pr = typing.cast(
            _TPTupleType[_CancelProto],
            await loop.create_unix_connection(proto_factory, addr)
        )
    else:
        if params.ssl and params.sslmode != SSLMode.allow:
            tr, pr = await _create_ssl_connection(
                proto_factory,
                *addr,
                loop=loop,
                ssl_context=params.ssl,
                ssl_is_advisory=params.sslmode == SSLMode.prefer)
        else:
            tr, pr = await loop.create_connection(proto_factory, *addr)
            _set_nodelay(_get_socket(tr))

    # Pack a CancelRequest message
    msg = struct.pack('!llll', 16, 80877102, backend_pid, backend_secret)

    try:
        tr.write(msg)
        await pr.on_disconnect
    finally:
        tr.close()


def _get_socket(transport: asyncio.BaseTransport) -> typing.Any:
    sock = transport.get_extra_info('socket')
    if sock is None:
        # Shouldn't happen with any asyncio-complaint event loop.
        raise ConnectionError(
            'could not get the socket for transport {!r}'.format(transport))
    return sock


def _set_nodelay(sock: typing.Any) -> None:
    if not hasattr(socket, 'AF_UNIX') or sock.family != socket.AF_UNIX:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def _create_future(
    loop: asyncio.AbstractEventLoop | None
) -> asyncio.Future[typing.Any]:
    try:
        create_future = loop.create_future  # type: ignore[union-attr]
    except AttributeError:
        return asyncio.Future(loop=loop)
    else:
        return create_future()
