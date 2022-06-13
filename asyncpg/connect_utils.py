# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import collections
import enum
import functools
import getpass
import os
import pathlib
import platform
import re
import socket
import ssl as ssl_module
import stat
import struct
import sys
import time
import typing
import urllib.parse
import warnings
import inspect

from . import compat
from . import exceptions
from . import protocol


class SSLMode(enum.IntEnum):
    disable = 0
    allow = 1
    prefer = 2
    require = 3
    verify_ca = 4
    verify_full = 5

    @classmethod
    def parse(cls, sslmode):
        if isinstance(sslmode, cls):
            return sslmode
        return getattr(cls, sslmode.replace('-', '_'))


_ConnectionParameters = collections.namedtuple(
    'ConnectionParameters',
    [
        'user',
        'password',
        'database',
        'ssl',
        'sslmode',
        'direct_tls',
        'connect_timeout',
        'server_settings',
    ])


_ClientConfiguration = collections.namedtuple(
    'ConnectionConfiguration',
    [
        'command_timeout',
        'statement_cache_size',
        'max_cached_statement_lifetime',
        'max_cacheable_statement_size',
    ])


_system = platform.uname().system


if _system == 'Windows':
    PGPASSFILE = 'pgpass.conf'
else:
    PGPASSFILE = '.pgpass'


def _read_password_file(passfile: pathlib.Path) \
        -> typing.List[typing.Tuple[str, ...]]:

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
        *, passfile: typing.Optional[pathlib.Path],
        hosts: typing.List[str],
        ports: typing.List[int],
        database: str,
        user: str):
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


def _validate_port_spec(hosts, port):
    if isinstance(port, list):
        # If there is a list of ports, its length must
        # match that of the host list.
        if len(port) != len(hosts):
            raise exceptions.InterfaceError(
                'could not match {} port numbers to {} hosts'.format(
                    len(port), len(hosts)))
    else:
        port = [port for _ in range(len(hosts))]

    return port


def _parse_hostlist(hostlist, port, *, unquote=False):
    if ',' in hostlist:
        # A comma-separated list of host addresses.
        hostspecs = hostlist.split(',')
    else:
        hostspecs = [hostlist]

    hosts = []
    hostlist_ports = []

    if not port:
        portspec = os.environ.get('PGPORT')
        if portspec:
            if ',' in portspec:
                default_port = [int(p) for p in portspec.split(',')]
            else:
                default_port = int(portspec)
        else:
            default_port = 5432

        default_port = _validate_port_spec(hostspecs, default_port)

    else:
        port = _validate_port_spec(hostspecs, port)

    for i, hostspec in enumerate(hostspecs):
        if hostspec[0] == '/':
            # Unix socket
            addr = hostspec
            hostspec_port = ''
        elif hostspec[0] == '[':
            # IPv6 address
            m = re.match(r'(?:\[([^\]]+)\])(?::([0-9]+))?', hostspec)
            if m:
                addr = m.group(1)
                hostspec_port = m.group(2)
            else:
                raise ValueError(
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

    if not port:
        port = hostlist_ports

    return hosts, port


def _parse_tls_version(tls_version):
    if not hasattr(ssl_module, 'TLSVersion'):
        raise ValueError(
            "TLSVersion is not supported in this version of Python"
        )
    if tls_version.startswith('SSL'):
        raise ValueError(
            f"Unsupported TLS version: {tls_version}"
        )
    try:
        return ssl_module.TLSVersion[tls_version.replace('.', '_')]
    except KeyError:
        raise ValueError(
            f"No such TLS version: {tls_version}"
        )


def _dot_postgresql_path(filename) -> pathlib.Path:
    return (pathlib.Path.home() / '.postgresql' / filename).resolve()


def _parse_connect_dsn_and_args(*, dsn, host, port, user,
                                password, passfile, database, ssl,
                                direct_tls, connect_timeout, server_settings):
    # `auth_hosts` is the version of host information for the purposes
    # of reading the pgpass file.
    auth_hosts = None
    sslcert = sslkey = sslrootcert = sslcrl = sslpassword = None
    ssl_min_protocol_version = ssl_max_protocol_version = None

    if dsn:
        parsed = urllib.parse.urlparse(dsn)

        if parsed.scheme not in {'postgresql', 'postgres'}:
            raise ValueError(
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
            query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
            for key, val in query.items():
                if isinstance(val, list):
                    query[key] = val[-1]

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
                if ssl is None:
                    ssl = val

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

    if not isinstance(host, list):
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

    port = _validate_port_spec(host, port)

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
        raise exceptions.InterfaceError(
            'could not determine user name to connect with')

    if database is None:
        raise exceptions.InterfaceError(
            'could not determine database name to connect to')

    if password is None:
        if passfile is None:
            passfile = os.getenv('PGPASSFILE')

        if passfile is None:
            homedir = compat.get_pg_home_directory()
            if homedir:
                passfile = homedir / PGPASSFILE
            else:
                passfile = None
        else:
            passfile = pathlib.Path(passfile)

        if passfile is not None:
            password = _read_password_from_pgpass(
                hosts=auth_hosts, ports=port,
                database=database, user=user,
                passfile=passfile)

    addrs = []
    have_tcp_addrs = False
    for h, p in zip(host, port):
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
        raise ValueError(
            'could not determine the database address to connect to')

    if ssl is None:
        ssl = os.getenv('PGSSLMODE')

    if ssl is None and have_tcp_addrs:
        ssl = 'prefer'

    if isinstance(ssl, (str, SSLMode)):
        try:
            sslmode = SSLMode.parse(ssl)
        except AttributeError:
            modes = ', '.join(m.name.replace('_', '-') for m in SSLMode)
            raise exceptions.InterfaceError(
                '`sslmode` parameter must be one of: {}'.format(modes))

        # docs at https://www.postgresql.org/docs/10/static/libpq-connect.html
        if sslmode < SSLMode.allow:
            ssl = False
        else:
            ssl = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS_CLIENT)
            ssl.check_hostname = sslmode >= SSLMode.verify_full
            if sslmode < SSLMode.require:
                ssl.verify_mode = ssl_module.CERT_NONE
            else:
                if sslrootcert is None:
                    sslrootcert = os.getenv('PGSSLROOTCERT')
                if sslrootcert:
                    ssl.load_verify_locations(cafile=sslrootcert)
                    ssl.verify_mode = ssl_module.CERT_REQUIRED
                else:
                    sslrootcert = _dot_postgresql_path('root.crt')
                    try:
                        ssl.load_verify_locations(cafile=sslrootcert)
                    except FileNotFoundError:
                        if sslmode > SSLMode.require:
                            raise ValueError(
                                f'root certificate file "{sslrootcert}" does '
                                f'not exist\nEither provide the file or '
                                f'change sslmode to disable server '
                                f'certificate verification.'
                            )
                        elif sslmode == SSLMode.require:
                            ssl.verify_mode = ssl_module.CERT_NONE
                        else:
                            assert False, 'unreachable'
                    else:
                        ssl.verify_mode = ssl_module.CERT_REQUIRED

                if sslcrl is None:
                    sslcrl = os.getenv('PGSSLCRL')
                if sslcrl:
                    ssl.load_verify_locations(cafile=sslcrl)
                    ssl.verify_flags |= ssl_module.VERIFY_CRL_CHECK_CHAIN
                else:
                    sslcrl = _dot_postgresql_path('root.crl')
                    try:
                        ssl.load_verify_locations(cafile=sslcrl)
                    except FileNotFoundError:
                        pass
                    else:
                        ssl.verify_flags |= ssl_module.VERIFY_CRL_CHECK_CHAIN

            if sslkey is None:
                sslkey = os.getenv('PGSSLKEY')
            if not sslkey:
                sslkey = _dot_postgresql_path('postgresql.key')
                if not sslkey.exists():
                    sslkey = None
            if not sslpassword:
                sslpassword = ''
            if sslcert is None:
                sslcert = os.getenv('PGSSLCERT')
            if sslcert:
                ssl.load_cert_chain(
                    sslcert, keyfile=sslkey, password=lambda: sslpassword
                )
            else:
                sslcert = _dot_postgresql_path('postgresql.crt')
                try:
                    ssl.load_cert_chain(
                        sslcert, keyfile=sslkey, password=lambda: sslpassword
                    )
                except FileNotFoundError:
                    pass

            # OpenSSL 1.1.1 keylog file, copied from create_default_context()
            if hasattr(ssl, 'keylog_filename'):
                keylogfile = os.environ.get('SSLKEYLOGFILE')
                if keylogfile and not sys.flags.ignore_environment:
                    ssl.keylog_filename = keylogfile

            if ssl_min_protocol_version is None:
                ssl_min_protocol_version = os.getenv('PGSSLMINPROTOCOLVERSION')
            if ssl_min_protocol_version:
                ssl.minimum_version = _parse_tls_version(
                    ssl_min_protocol_version
                )
            else:
                try:
                    ssl.minimum_version = _parse_tls_version('TLSv1.2')
                except ValueError:
                    # Python 3.6 does not have ssl.TLSVersion
                    pass

            if ssl_max_protocol_version is None:
                ssl_max_protocol_version = os.getenv('PGSSLMAXPROTOCOLVERSION')
            if ssl_max_protocol_version:
                ssl.maximum_version = _parse_tls_version(
                    ssl_max_protocol_version
                )

    elif ssl is True:
        ssl = ssl_module.create_default_context()
        sslmode = SSLMode.verify_full
    else:
        sslmode = SSLMode.disable

    if server_settings is not None and (
            not isinstance(server_settings, dict) or
            not all(isinstance(k, str) for k in server_settings) or
            not all(isinstance(v, str) for v in server_settings.values())):
        raise ValueError(
            'server_settings is expected to be None or '
            'a Dict[str, str]')

    params = _ConnectionParameters(
        user=user, password=password, database=database, ssl=ssl,
        sslmode=sslmode, direct_tls=direct_tls,
        connect_timeout=connect_timeout, server_settings=server_settings)

    return addrs, params


def _parse_connect_arguments(*, dsn, host, port, user, password, passfile,
                             database, timeout, command_timeout,
                             statement_cache_size,
                             max_cached_statement_lifetime,
                             max_cacheable_statement_size,
                             ssl, direct_tls, server_settings):

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
        connect_timeout=timeout, server_settings=server_settings)

    config = _ClientConfiguration(
        command_timeout=command_timeout,
        statement_cache_size=statement_cache_size,
        max_cached_statement_lifetime=max_cached_statement_lifetime,
        max_cacheable_statement_size=max_cacheable_statement_size,)

    return addrs, params, config


class TLSUpgradeProto(asyncio.Protocol):
    def __init__(self, loop, host, port, ssl_context, ssl_is_advisory):
        self.on_data = _create_future(loop)
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.ssl_is_advisory = ssl_is_advisory

    def data_received(self, data):
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

    def connection_lost(self, exc):
        if not self.on_data.done():
            if exc is None:
                exc = ConnectionError('unexpected connection_lost() call')
            self.on_data.set_exception(exc)


async def _create_ssl_connection(protocol_factory, host, port, *,
                                 loop, ssl_context, ssl_is_advisory=False):

    tr, pr = await loop.create_connection(
        lambda: TLSUpgradeProto(loop, host, port,
                                ssl_context, ssl_is_advisory),
        host, port)

    tr.write(struct.pack('!ll', 8, 80877103))  # SSLRequest message.

    try:
        do_ssl_upgrade = await pr.on_data
    except (Exception, asyncio.CancelledError):
        tr.close()
        raise

    if hasattr(loop, 'start_tls'):
        if do_ssl_upgrade:
            try:
                new_tr = await loop.start_tls(
                    tr, pr, ssl_context, server_hostname=host)
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
    addr,
    loop,
    timeout,
    params,
    config,
    connection_class,
    record_class
):
    assert loop is not None

    if timeout <= 0:
        raise asyncio.TimeoutError

    params_input = params
    if callable(params.password):
        password = params.password()
        if inspect.isawaitable(password):
            password = await password

        params = params._replace(password=password)
    args = (addr, loop, config, connection_class, record_class, params_input)

    # prepare the params (which attempt has ssl) for the 2 attempts
    if params.sslmode == SSLMode.allow:
        params_retry = params
        params = params._replace(ssl=None)
    elif params.sslmode == SSLMode.prefer:
        params_retry = params._replace(ssl=None)
    else:
        # skip retry if we don't have to
        return await __connect_addr(params, timeout, False, *args)

    # first attempt
    before = time.monotonic()
    try:
        return await __connect_addr(params, timeout, True, *args)
    except _RetryConnectSignal:
        pass

    # second attempt
    timeout -= time.monotonic() - before
    if timeout <= 0:
        raise asyncio.TimeoutError
    else:
        return await __connect_addr(params_retry, timeout, False, *args)


class _RetryConnectSignal(Exception):
    pass


async def __connect_addr(
    params,
    timeout,
    retry,
    addr,
    loop,
    config,
    connection_class,
    record_class,
    params_input,
):
    connected = _create_future(loop)

    proto_factory = lambda: protocol.Protocol(
        addr, connected, params, record_class, loop)

    if isinstance(addr, str):
        # UNIX socket
        connector = loop.create_unix_connection(proto_factory, addr)

    elif params.ssl and params.direct_tls:
        # if ssl and direct_tls are given, skip STARTTLS and perform direct
        # SSL connection
        connector = loop.create_connection(
            proto_factory, *addr, ssl=params.ssl
        )

    elif params.ssl:
        connector = _create_ssl_connection(
            proto_factory, *addr, loop=loop, ssl_context=params.ssl,
            ssl_is_advisory=params.sslmode == SSLMode.prefer)
    else:
        connector = loop.create_connection(proto_factory, *addr)

    connector = asyncio.ensure_future(connector)
    before = time.monotonic()
    tr, pr = await compat.wait_for(connector, timeout=timeout)
    timeout -= time.monotonic() - before

    try:
        if timeout <= 0:
            raise asyncio.TimeoutError
        await compat.wait_for(connected, timeout=timeout)
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


async def _connect(*, loop, timeout, connection_class, record_class, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    addrs, params, config = _parse_connect_arguments(timeout=timeout, **kwargs)

    last_error = None
    addr = None
    for addr in addrs:
        before = time.monotonic()
        try:
            return await _connect_addr(
                addr=addr,
                loop=loop,
                timeout=timeout,
                params=params,
                config=config,
                connection_class=connection_class,
                record_class=record_class,
            )
        except (OSError, asyncio.TimeoutError, ConnectionError) as ex:
            last_error = ex
        finally:
            timeout -= time.monotonic() - before

    raise last_error


async def _cancel(*, loop, addr, params: _ConnectionParameters,
                  backend_pid, backend_secret):

    class CancelProto(asyncio.Protocol):

        def __init__(self):
            self.on_disconnect = _create_future(loop)
            self.is_ssl = False

        def connection_lost(self, exc):
            if not self.on_disconnect.done():
                self.on_disconnect.set_result(True)

    if isinstance(addr, str):
        tr, pr = await loop.create_unix_connection(CancelProto, addr)
    else:
        if params.ssl and params.sslmode != SSLMode.allow:
            tr, pr = await _create_ssl_connection(
                CancelProto,
                *addr,
                loop=loop,
                ssl_context=params.ssl,
                ssl_is_advisory=params.sslmode == SSLMode.prefer)
        else:
            tr, pr = await loop.create_connection(
                CancelProto, *addr)
            _set_nodelay(_get_socket(tr))

    # Pack a CancelRequest message
    msg = struct.pack('!llll', 16, 80877102, backend_pid, backend_secret)

    try:
        tr.write(msg)
        await pr.on_disconnect
    finally:
        tr.close()


def _get_socket(transport):
    sock = transport.get_extra_info('socket')
    if sock is None:
        # Shouldn't happen with any asyncio-complaint event loop.
        raise ConnectionError(
            'could not get the socket for transport {!r}'.format(transport))
    return sock


def _set_nodelay(sock):
    if not hasattr(socket, 'AF_UNIX') or sock.family != socket.AF_UNIX:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def _create_future(loop):
    try:
        create_future = loop.create_future
    except AttributeError:
        return asyncio.Future(loop=loop)
    else:
        return create_future()
