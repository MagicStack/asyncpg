# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import collections
import getpass
import os
import socket
import struct
import time
import urllib.parse

from . import exceptions
from . import protocol


_ConnectionParameters = collections.namedtuple(
    'ConnectionParameters',
    [
        'user',
        'password',
        'database',
        'ssl',
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


def _parse_connect_dsn_and_args(*, dsn, host, port, user,
                                password, database, ssl, connect_timeout,
                                server_settings):
    if host is not None and not isinstance(host, str):
        raise TypeError(
            'host argument is expected to be str, got {!r}'.format(
                type(host)))

    if dsn:
        parsed = urllib.parse.urlparse(dsn)

        if parsed.scheme not in {'postgresql', 'postgres'}:
            raise ValueError(
                'invalid DSN: scheme is expected to be either of '
                '"postgresql" or "postgres", got {!r}'.format(parsed.scheme))

        if parsed.port and port is None:
            port = int(parsed.port)

        if parsed.hostname and host is None:
            host = parsed.hostname

        if parsed.path and database is None:
            database = parsed.path
            if database.startswith('/'):
                database = database[1:]

        if parsed.username and user is None:
            user = parsed.username

        if parsed.password and password is None:
            password = parsed.password

        if parsed.query:
            query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
            for key, val in query.items():
                if isinstance(val, list):
                    query[key] = val[-1]

            if 'host' in query:
                val = query.pop('host')
                if host is None:
                    host = val

            if 'port' in query:
                val = int(query.pop('port'))
                if port is None:
                    port = val

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

            if query:
                if server_settings is None:
                    server_settings = query
                else:
                    server_settings = {**query, **server_settings}

    # On env-var -> connection parameter conversion read here:
    # https://www.postgresql.org/docs/current/static/libpq-envars.html
    # Note that env values may be an empty string in cases when
    # the variable is "unset" by setting it to an empty value
    #
    if host is None:
        host = os.getenv('PGHOST')
        if not host:
            host = ['/tmp', '/private/tmp',
                    '/var/pgsql_socket', '/run/postgresql',
                    'localhost']
    if not isinstance(host, list):
        host = [host]

    if port is None:
        port = os.getenv('PGPORT')
        if port:
            port = int(port)
        else:
            port = 5432
    else:
        port = int(port)

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

    addrs = []
    for h in host:
        if h.startswith('/'):
            # UNIX socket name
            if '.s.PGSQL.' not in h:
                h = os.path.join(h, '.s.PGSQL.{}'.format(port))
            addrs.append(h)
        else:
            # TCP host/port
            addrs.append((h, port))

    if not addrs:
        raise ValueError(
            'could not determine the database address to connect to')

    if ssl:
        for addr in addrs:
            if isinstance(addr, str):
                # UNIX socket
                raise exceptions.InterfaceError(
                    '`ssl` parameter can only be enabled for TCP addresses, '
                    'got a UNIX socket path: {!r}'.format(addr))

    if server_settings is not None and (
            not isinstance(server_settings, dict) or
            not all(isinstance(k, str) for k in server_settings) or
            not all(isinstance(v, str) for v in server_settings.values())):
        raise ValueError(
            'server_settings is expected to be None or '
            'a Dict[str, str]')

    params = _ConnectionParameters(
        user=user, password=password, database=database, ssl=ssl,
        connect_timeout=connect_timeout, server_settings=server_settings)

    return addrs, params


def _parse_connect_arguments(*, dsn, host, port, user, password, database,
                             timeout, command_timeout, statement_cache_size,
                             max_cached_statement_lifetime,
                             max_cacheable_statement_size,
                             ssl, server_settings):

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
        password=password, ssl=ssl,
        database=database, connect_timeout=timeout,
        server_settings=server_settings)

    config = _ClientConfiguration(
        command_timeout=command_timeout,
        statement_cache_size=statement_cache_size,
        max_cached_statement_lifetime=max_cached_statement_lifetime,
        max_cacheable_statement_size=max_cacheable_statement_size,)

    return addrs, params, config


async def _connect_addr(*, addr, loop, timeout, params, config,
                        connection_class):
    assert loop is not None

    if timeout <= 0:
        raise asyncio.TimeoutError

    connected = _create_future(loop)
    proto_factory = lambda: protocol.Protocol(
        addr, connected, params, loop)

    if isinstance(addr, str):
        # UNIX socket
        assert params.ssl is None
        connector = loop.create_unix_connection(proto_factory, addr)
    elif params.ssl:
        connector = _create_ssl_connection(
            proto_factory, *addr, loop=loop, ssl_context=params.ssl)
    else:
        connector = loop.create_connection(proto_factory, *addr)

    before = time.monotonic()
    tr, pr = await asyncio.wait_for(
        connector, timeout=timeout, loop=loop)
    timeout -= time.monotonic() - before

    try:
        if timeout <= 0:
            raise asyncio.TimeoutError
        await asyncio.wait_for(connected, loop=loop, timeout=timeout)
    except Exception:
        tr.close()
        raise

    con = connection_class(pr, tr, loop, addr, config, params)
    pr.set_connection(con)
    return con


async def _connect(*, loop, timeout, connection_class, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    addrs, params, config = _parse_connect_arguments(timeout=timeout, **kwargs)

    last_error = None
    addr = None
    for addr in addrs:
        before = time.monotonic()
        try:
            con = await _connect_addr(
                addr=addr, loop=loop, timeout=timeout,
                params=params, config=config,
                connection_class=connection_class)
        except (OSError, asyncio.TimeoutError, ConnectionError) as ex:
            last_error = ex
        else:
            return con
        finally:
            timeout -= time.monotonic() - before

    raise last_error


async def _get_ssl_ready_socket(host, port, *, loop):
    reader, writer = await asyncio.open_connection(host, port, loop=loop)

    tr = writer.transport
    try:
        sock = _get_socket(tr)
        _set_nodelay(sock)

        writer.write(struct.pack('!ll', 8, 80877103))  # SSLRequest message.
        await writer.drain()
        resp = await reader.readexactly(1)

        if resp == b'S':
            return sock.dup()
        else:
            raise ConnectionError(
                'PostgreSQL server at "{}:{}" rejected SSL upgrade'.format(
                    host, port))
    finally:
        tr.close()


async def _create_ssl_connection(protocol_factory, host, port, *,
                                 loop, ssl_context):
    sock = await _get_ssl_ready_socket(host, port, loop=loop)
    try:
        return await loop.create_connection(
            protocol_factory, sock=sock, ssl=ssl_context,
            server_hostname=host)
    except Exception:
        sock.close()
        raise


async def _open_connection(*, loop, addr, params: _ConnectionParameters):
    if isinstance(addr, str):
        r, w = await asyncio.open_unix_connection(addr, loop=loop)
    else:
        if params.ssl:
            sock = await _get_ssl_ready_socket(*addr, loop=loop)

            try:
                r, w = await asyncio.open_connection(
                    sock=sock,
                    loop=loop,
                    ssl=params.ssl,
                    server_hostname=addr[0])
            except Exception:
                sock.close()
                raise

        else:
            r, w = await asyncio.open_connection(*addr, loop=loop)
            _set_nodelay(_get_socket(w.transport))

    return r, w


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
