# Copyright (C) 2016-present the ayncpg authors and contributors
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
import urllib.parse

from . import cursor
from . import introspection
from . import prepared_stmt
from . import protocol
from . import serverversion
from . import transaction


class Connection:
    """A representation of a database session.

    Connections are created by calling :func:`~asyncpg.connection.connect`.
    """

    __slots__ = ('_protocol', '_transport', '_loop', '_types_stmt',
                 '_type_by_name_stmt', '_top_xact', '_uid', '_aborted',
                 '_stmt_cache_max_size', '_stmt_cache', '_stmts_to_close',
                 '_addr', '_opts', '_command_timeout', '_listeners',
                 '_server_version', '_intro_query')

    def __init__(self, protocol, transport, loop, addr, opts, *,
                 statement_cache_size, command_timeout):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop
        self._types_stmt = None
        self._type_by_name_stmt = None
        self._top_xact = None
        self._uid = 0
        self._aborted = False

        self._addr = addr
        self._opts = opts

        self._stmt_cache_max_size = statement_cache_size
        self._stmt_cache = collections.OrderedDict()
        self._stmts_to_close = set()

        self._command_timeout = command_timeout

        self._listeners = {}

        ver_string = self._protocol.get_settings().server_version
        self._server_version = \
            serverversion.split_server_version_string(ver_string)

        if self._server_version < (9, 2):
            self._intro_query = introspection.INTRO_LOOKUP_TYPES_91
        else:
            self._intro_query = introspection.INTRO_LOOKUP_TYPES

    async def add_listener(self, channel, callback):
        """Add a listener for Postgres notifications.

        :param str channel: Channel to listen on.
        :param callable callback:
                A callable receiving the following arguments:
                **connection**: a Connection the callback is registered with;
                **pid**: PID of the Postgres server that sent the notification;
                **channel**: name of the channel the notification was sent to;
                **payload**: the payload.
        """
        if channel not in self._listeners:
            await self.fetch('LISTEN {}'.format(channel))
            self._listeners[channel] = set()
        self._listeners[channel].add(callback)

    async def remove_listener(self, channel, callback):
        """Remove a listening callback on the specified channel."""
        if channel not in self._listeners:
            return
        if callback not in self._listeners[channel]:
            return
        self._listeners[channel].remove(callback)
        if not self._listeners[channel]:
            del self._listeners[channel]
            await self.fetch('UNLISTEN {}'.format(channel))

    def get_server_pid(self):
        """Return the PID of the Postgres server the connection is bound to."""
        return self._protocol.get_server_pid()

    def get_server_version(self):
        """Return the version of the connected PostgreSQL server.

        The returned value is a named tuple similar to that in
        ``sys.version_info``:

        .. code-block:: pycon

            >>> con.get_server_version()
            ServerVersion(major=9, minor=6, micro=1,
                          releaselevel='final', serial=0)

        """
        return self._server_version

    def get_settings(self):
        """Return connection settings.

        :return: :class:`~asyncpg.ConnectionSettings`.
        """
        return self._protocol.get_settings()

    def transaction(self, *, isolation='read_committed', readonly=False,
                    deferrable=False):
        """Create a :class:`~transaction.Transaction` object.

        Refer to `PostgreSQL documentation`_ on the meaning of transaction
        parameters.

        :param isolation: Transaction isolation mode, can be one of:
                          `'serializable'`, `'repeatable_read'`,
                          `'read_committed'`.

        :param readonly: Specifies whether or not this transaction is
                         read-only.

        :param deferrable: Specifies whether or not this transaction is
                           deferrable.

        .. _`PostgreSQL documentation`: https://www.postgresql.org/docs/current/static/sql-set-transaction.html
        """
        return transaction.Transaction(self, isolation, readonly, deferrable)

    async def execute(self, query: str, *args, timeout: float=None) -> str:
        """Execute an SQL command (or commands).

        This method can execute many SQL commands at once, when no arguments
        are provided.

        Example:

        .. code-block:: pycon

            >>> await con.execute('''
            ...     CREATE TABLE mytab (a int);
            ...     INSERT INTO mytab (a) VALUES (100), (200), (300);
            ... ''')
            INSERT 0 3

            >>> await con.execute('''
            ...     INSERT INTO mytab (a) VALUES ($1), ($2)
            ... ''', 10, 20)
            INSERT 0 2

        :param args: Query arguments.
        :param float timeout: Optional timeout value in seconds.
        :return str: Status of the last SQL command.

        .. versionchanged:: 0.5.4
           Made it possible to pass query arguments.
        """
        if not args:
            return await self._protocol.query(query, timeout)

        stmt = await self._get_statement(query, timeout)
        _, status, _ = await self._protocol.bind_execute(stmt, args, '', 0,
                                                         True, timeout)
        return status.decode()

    async def executemany(self, command: str, args, timeout: float=None):
        """Execute an SQL *command* for each sequence of arguments in *args*.

        Example:

        .. code-block:: pycon

            >>> await con.executemany('''
            ...     INSERT INTO mytab (a) VALUES ($1, $2, $3);
            ... ''', [(1, 2, 3), (4, 5, 6)])

        :param command: Command to execute.
        :args: An iterable containing sequences of arguments.
        :param float timeout: Optional timeout value in seconds.
        :return None: This method discards the results of the operations.

        .. versionadded:: 0.7.0
        """
        stmt = await self._get_statement(command, timeout)
        return await self._protocol.bind_execute_many(stmt, args, '', timeout)

    async def _get_statement(self, query, timeout):
        cache = self._stmt_cache_max_size > 0

        if cache:
            try:
                state = self._stmt_cache[query]
            except KeyError:
                pass
            else:
                self._stmt_cache.move_to_end(query, last=True)
                if not state.closed:
                    return state

        protocol = self._protocol
        state = await protocol.prepare(None, query, timeout)

        ready = state._init_types()
        if ready is not True:
            if self._types_stmt is None:
                self._types_stmt = await self.prepare(self._intro_query)

            types = await self._types_stmt.fetch(list(ready))
            protocol.get_settings().register_data_types(types)

        if cache:
            if len(self._stmt_cache) > self._stmt_cache_max_size - 1:
                old_query, old_state = self._stmt_cache.popitem(last=False)
                self._maybe_gc_stmt(old_state)
            self._stmt_cache[query] = state

        # If we've just created a new statement object, check if there
        # are any statements for GC.
        if self._stmts_to_close:
            await self._cleanup_stmts()

        return state

    def cursor(self, query, *args, prefetch=None, timeout=None):
        """Return a *cursor factory* for the specified query.

        :param args: Query arguments.
        :param int prefetch: The number of rows the *cursor iterator*
                             will prefetch (defaults to ``50``.)
        :param float timeout: Optional timeout in seconds.

        :return: A :class:`~cursor.CursorFactory` object.
        """
        return cursor.CursorFactory(self, query, None, args,
                                    prefetch, timeout)

    async def prepare(self, query, *, timeout=None):
        """Create a *prepared statement* for the specified query.

        :param str query: Text of the query to create a prepared statement for.
        :param float timeout: Optional timeout value in seconds.

        :return: A :class:`~prepared_stmt.PreparedStatement` instance.
        """
        stmt = await self._get_statement(query, timeout)
        return prepared_stmt.PreparedStatement(self, query, stmt)

    async def fetch(self, query, *args, timeout=None) -> list:
        """Run a query and return the results as a list of :class:`Record`.

        :param str query: Query text.
        :param args: Query arguments.
        :param float timeout: Optional timeout value in seconds.

        :return list: A list of :class:`Record` instances.
        """
        stmt = await self._get_statement(query, timeout)
        data = await self._protocol.bind_execute(stmt, args, '', 0,
                                                 False, timeout)
        return data

    async def fetchval(self, query, *args, column=0, timeout=None):
        """Run a query and return a value in the first row.

        :param str query: Query text.
        :param args: Query arguments.
        :param int column: Numeric index within the record of the value to
                           return (defaults to 0).
        :param float timeout: Optional timeout value in seconds.
                            If not specified, defaults to the value of
                            ``command_timeout`` argument to the ``Connection``
                            instance constructor.

        :return: The value of the specified column of the first record.
        """
        stmt = await self._get_statement(query, timeout)
        data = await self._protocol.bind_execute(stmt, args, '', 1,
                                                 False, timeout)
        if not data:
            return None
        return data[0][column]

    async def fetchrow(self, query, *args, timeout=None):
        """Run a query and return the first row.

        :param str query: Query text
        :param args: Query arguments
        :param float timeout: Optional timeout value in seconds.

        :return: The first row as a :class:`Record` instance.
        """
        stmt = await self._get_statement(query, timeout)
        data = await self._protocol.bind_execute(stmt, args, '', 1,
                                                 False, timeout)
        if not data:
            return None
        return data[0]

    async def set_type_codec(self, typename, *,
                             schema='public', encoder, decoder, binary=False):
        """Set an encoder/decoder pair for the specified data type.

        :param typename:  Name of the data type the codec is for.
        :param schema:  Schema name of the data type the codec is for
                        (defaults to 'public')
        :param encoder:  Callable accepting a single argument and returning
                         a string or a bytes object (if `binary` is True).
        :param decoder:  Callable accepting a single string or bytes argument
                         and returning a decoded object.
        :param binary:  Specifies whether the codec is able to handle binary
                        data.  If ``False`` (the default), the data is
                        expected to be encoded/decoded in text.
        """
        if self._type_by_name_stmt is None:
            self._type_by_name_stmt = await self.prepare(
                introspection.TYPE_BY_NAME)

        typeinfo = await self._type_by_name_stmt.fetchrow(
            typename, schema)
        if not typeinfo:
            raise ValueError('unknown type: {}.{}'.format(schema, typename))

        oid = typeinfo['oid']
        if typeinfo['kind'] != b'b' or typeinfo['elemtype']:
            raise ValueError(
                'cannot use custom codec on non-scalar type {}.{}'.format(
                    schema, typename))

        self._protocol.get_settings().add_python_codec(
            oid, typename, schema, 'scalar',
            encoder, decoder, binary)

    async def set_builtin_type_codec(self, typename, *,
                                     schema='public', codec_name):
        """Set a builtin codec for the specified data type.

        :param typename:  Name of the data type the codec is for.
        :param schema:  Schema name of the data type the codec is for
                        (defaults to 'public')
        :param codec_name:  The name of the builtin codec.
        """
        if self._type_by_name_stmt is None:
            self._type_by_name_stmt = await self.prepare(
                introspection.TYPE_BY_NAME)

        typeinfo = await self._type_by_name_stmt.fetchrow(
            typename, schema)
        if not typeinfo:
            raise ValueError('unknown type: {}.{}'.format(schema, typename))

        oid = typeinfo['oid']
        if typeinfo['kind'] != b'b' or typeinfo['elemtype']:
            raise ValueError(
                'cannot alias non-scalar type {}.{}'.format(
                    schema, typename))

        self._protocol.get_settings().set_builtin_type_codec(
            oid, typename, schema, 'scalar', codec_name)

    def is_closed(self):
        """Return ``True`` if the connection is closed, ``False`` otherwise.

        :return bool: ``True`` if the connection is closed, ``False``
                      otherwise.
        """
        return not self._protocol.is_connected() or self._aborted

    async def close(self):
        """Close the connection gracefully."""
        if self.is_closed():
            return
        self._close_stmts()
        self._listeners = {}
        self._aborted = True
        await self._protocol.close()

    def terminate(self):
        """Terminate the connection without waiting for pending data."""
        self._close_stmts()
        self._listeners = {}
        self._aborted = True
        self._protocol.abort()

    async def reset(self):
        self._listeners = {}

        await self.execute('''
            DO $$
            BEGIN
                PERFORM * FROM pg_listening_channels() LIMIT 1;
                IF FOUND THEN
                    UNLISTEN *;
                END IF;
            END;
            $$;
            SET SESSION AUTHORIZATION DEFAULT;
            RESET ALL;
            CLOSE ALL;
            SELECT pg_advisory_unlock_all();
        ''')

    def _get_unique_id(self):
        self._uid += 1
        return 'id{}'.format(self._uid)

    def _close_stmts(self):
        for stmt in self._stmt_cache.values():
            stmt.mark_closed()

        for stmt in self._stmts_to_close:
            stmt.mark_closed()

        self._stmt_cache.clear()
        self._stmts_to_close.clear()

    def _maybe_gc_stmt(self, stmt):
        if stmt.refs == 0 and stmt.query not in self._stmt_cache:
            stmt.mark_closed()
            self._stmts_to_close.add(stmt)

    async def _cleanup_stmts(self):
        to_close = self._stmts_to_close
        self._stmts_to_close = set()
        for stmt in to_close:
            await self._protocol.close_statement(stmt, False)

    def _request_portal_name(self):
        return self._get_unique_id()

    def _cancel_current_command(self, waiter):
        async def cancel():
            try:
                # Open new connection to the server
                if isinstance(self._addr, str):
                    r, w = await asyncio.open_unix_connection(
                        self._addr, loop=self._loop)
                else:
                    r, w = await asyncio.open_connection(
                        *self._addr, loop=self._loop)

                    sock = w.transport.get_extra_info('socket')
                    sock.setsockopt(socket.IPPROTO_TCP,
                                    socket.TCP_NODELAY, 1)

                # Pack CancelRequest message
                msg = struct.pack('!llll', 16, 80877102,
                                  self._protocol.backend_pid,
                                  self._protocol.backend_secret)
            except Exception as ex:
                waiter.set_exception(ex)
                return

            try:
                w.write(msg)
                await r.read()  # Wait until EOF
            except ConnectionResetError:
                # On some systems Postgres will reset the connection
                # after processing the cancellation command.
                pass
            except Exception as ex:
                waiter.set_exception(ex)
            finally:
                if not waiter.done():  # Ensure set_exception wasn't called.
                    waiter.set_result(None)
                w.close()

        self._loop.create_task(cancel())

    def _notify(self, pid, channel, payload):
        if channel not in self._listeners:
            return

        for cb in self._listeners[channel]:
            try:
                cb(self, pid, channel, payload)
            except Exception as ex:
                self._loop.call_exception_handler({
                    'message': 'Unhandled exception in asyncpg notification '
                               'listener callback {!r}'.format(cb),
                    'exception': ex
                })


async def connect(dsn=None, *,
                  host=None, port=None,
                  user=None, password=None,
                  database=None,
                  loop=None,
                  timeout=60,
                  statement_cache_size=100,
                  command_timeout=None,
                  **opts):
    """A coroutine to establish a connection to a PostgreSQL server.

    Returns a new :class:`~asyncpg.connection.Connection` object.

    :param dsn: Connection arguments specified using as a single string in the
                following format:
                ``postgres://user:pass@host:port/database?option=value``

    :param host: database host address or a path to the directory containing
                 database server UNIX socket (defaults to the default UNIX
                 socket, or the value of the ``PGHOST`` environment variable,
                 if set).

    :param port: connection port number (defaults to ``5432``, or the value of
                 the ``PGPORT`` environment variable, if set)

    :param user: the name of the database role used for authentication
                 (defaults to the name of the effective user of the process
                 making the connection, or the value of ``PGUSER`` environment
                 variable, if set)

    :param database: the name of the database (defaults to the value of
                     ``PGDATABASE`` environment variable, if set.)

    :param password: password used for authentication

    :param loop: An asyncio event loop instance.  If ``None``, the default
                 event loop will be used.

    :param float timeout: connection timeout in seconds.

    :param float command_timeout: the default timeout for operations on
                          this connection (the default is no timeout).

    :param int statement_cache_size: the size of prepared statement LRU cache.

    :return: A :class:`~asyncpg.connection.Connection` instance.

    Example:

    .. code-block:: pycon

        >>> import asyncpg
        >>> import asyncio
        >>> async def run():
        ...     con = await asyncpg.connect(user='postgres')
        ...     types = await con.fetch('SELECT * FROM pg_type')
        ...     print(types)
        >>> asyncio.get_event_loop().run_until_complete(run())
        [<Record typname='bool' typnamespace=11 ...
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    host, port, opts = _parse_connect_params(
        dsn=dsn, host=host, port=port, user=user, password=password,
        database=database, opts=opts)

    last_ex = None
    addr = None
    for h in host:
        connected = _create_future(loop)
        unix = h.startswith('/')

        if unix:
            # UNIX socket name
            addr = h
            if '.s.PGSQL.' not in addr:
                addr = os.path.join(addr, '.s.PGSQL.{}'.format(port))
            conn = loop.create_unix_connection(
                lambda: protocol.Protocol(addr, connected, opts, loop),
                addr)
        else:
            addr = (h, port)
            conn = loop.create_connection(
                lambda: protocol.Protocol(addr, connected, opts, loop),
                h, port)

        try:
            tr, pr = await asyncio.wait_for(conn, timeout=timeout, loop=loop)
        except (OSError, asyncio.TimeoutError) as ex:
            last_ex = ex
        else:
            break
    else:
        raise last_ex

    try:
        await connected
    except:
        tr.close()
        raise

    con = Connection(pr, tr, loop, addr, opts,
                     statement_cache_size=statement_cache_size,
                     command_timeout=command_timeout)
    pr.set_connection(con)
    return con


def _parse_connect_params(*, dsn, host, port, user,
                          password, database, opts):

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
                opts = {**query, **opts}

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

    if user is not None:
        opts['user'] = user
    if password is not None:
        opts['password'] = password
    if database is not None:
        opts['database'] = database

    for param in opts:
        if not isinstance(param, str):
            raise ValueError(
                'invalid connection parameter {!r} (str expected)'
                .format(param))
        if not isinstance(opts[param], str):
            raise ValueError(
                'invalid connection parameter {!r}: {!r} (str expected)'
                .format(param, opts[param]))

    return host, port, opts


def _create_future(loop):
    try:
        create_future = loop.create_future
    except AttributeError:
        return asyncio.Future(loop=loop)
    else:
        return create_future()
