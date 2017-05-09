# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import collections
import struct
import time

from . import connect_utils
from . import cursor
from . import exceptions
from . import introspection
from . import prepared_stmt
from . import protocol
from . import serverversion
from . import transaction


class ConnectionMeta(type):

    def __instancecheck__(cls, instance):
        mro = type(instance).__mro__
        return Connection in mro or _ConnectionProxy in mro


class Connection(metaclass=ConnectionMeta):
    """A representation of a database session.

    Connections are created by calling :func:`~asyncpg.connection.connect`.
    """

    __slots__ = ('_protocol', '_transport', '_loop', '_types_stmt',
                 '_type_by_name_stmt', '_top_xact', '_uid', '_aborted',
                 '_stmt_cache', '_stmts_to_close', '_listeners',
                 '_server_version', '_server_caps', '_intro_query',
                 '_reset_query', '_proxy', '_stmt_exclusive_section',
                 '_config', '_params', '_addr')

    def __init__(self, protocol, transport, loop,
                 addr: (str, int) or str,
                 config: connect_utils._ClientConfiguration,
                 params: connect_utils._ConnectionParameters):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop
        self._types_stmt = None
        self._type_by_name_stmt = None
        self._top_xact = None
        self._uid = 0
        self._aborted = False

        self._addr = addr
        self._config = config
        self._params = params

        self._stmt_cache = _StatementCache(
            loop=loop,
            max_size=config.statement_cache_size,
            on_remove=self._maybe_gc_stmt,
            max_lifetime=config.max_cached_statement_lifetime)

        self._stmts_to_close = set()

        self._listeners = {}

        settings = self._protocol.get_settings()
        ver_string = settings.server_version
        self._server_version = \
            serverversion.split_server_version_string(ver_string)

        self._server_caps = _detect_server_capabilities(
            self._server_version, settings)

        if self._server_version < (9, 2):
            self._intro_query = introspection.INTRO_LOOKUP_TYPES_91
        else:
            self._intro_query = introspection.INTRO_LOOKUP_TYPES

        self._reset_query = None
        self._proxy = None

        # Used to serialize operations that might involve anonymous
        # statements.  Specifically, we want to make the following
        # operation atomic:
        #    ("prepare an anonymous statement", "use the statement")
        #
        # Used for `con.fetchval()`, `con.fetch()`, `con.fetchrow()`,
        # `con.execute()`, and `con.executemany()`.
        self._stmt_exclusive_section = _Atomic()

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
        self._check_open()
        if channel not in self._listeners:
            await self.fetch('LISTEN {}'.format(channel))
            self._listeners[channel] = set()
        self._listeners[channel].add(callback)

    async def remove_listener(self, channel, callback):
        """Remove a listening callback on the specified channel."""
        if self.is_closed():
            return
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

        .. versionadded:: 0.8.0
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

        .. _`PostgreSQL documentation`: https://www.postgresql.org/docs/\
                                        current/static/sql-set-transaction.html
        """
        self._check_open()
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
        self._check_open()

        if not args:
            return await self._protocol.query(query, timeout)

        _, status, _ = await self._execute(query, args, 0, timeout, True)
        return status.decode()

    async def executemany(self, command: str, args, *, timeout: float=None):
        """Execute an SQL *command* for each sequence of arguments in *args*.

        Example:

        .. code-block:: pycon

            >>> await con.executemany('''
            ...     INSERT INTO mytab (a) VALUES ($1, $2, $3);
            ... ''', [(1, 2, 3), (4, 5, 6)])

        :param command: Command to execute.
        :param args: An iterable containing sequences of arguments.
        :param float timeout: Optional timeout value in seconds.
        :return None: This method discards the results of the operations.

        .. versionadded:: 0.7.0

        .. versionchanged:: 0.11.0
           `timeout` became a keyword-only parameter.
        """
        self._check_open()
        return await self._executemany(command, args, timeout)

    async def _get_statement(self, query, timeout, *, named: bool=False):
        statement = self._stmt_cache.get(query)
        if statement is not None:
            return statement

        # Only use the cache when:
        #  * `statement_cache_size` is greater than 0;
        #  * query size is less than `max_cacheable_statement_size`.
        use_cache = self._stmt_cache.get_max_size() > 0
        if (use_cache and
                self._config.max_cacheable_statement_size and
                len(query) > self._config.max_cacheable_statement_size):
            use_cache = False

        if use_cache or named:
            stmt_name = self._get_unique_id('stmt')
        else:
            stmt_name = ''

        statement = await self._protocol.prepare(stmt_name, query, timeout)

        ready = statement._init_types()
        if ready is not True:
            if self._types_stmt is None:
                self._types_stmt = await self.prepare(self._intro_query)

            types = await self._types_stmt.fetch(list(ready))
            self._protocol.get_settings().register_data_types(types)

        if use_cache:
            self._stmt_cache.put(query, statement)

        # If we've just created a new statement object, check if there
        # are any statements for GC.
        if self._stmts_to_close:
            await self._cleanup_stmts()

        return statement

    def cursor(self, query, *args, prefetch=None, timeout=None):
        """Return a *cursor factory* for the specified query.

        :param args: Query arguments.
        :param int prefetch: The number of rows the *cursor iterator*
                             will prefetch (defaults to ``50``.)
        :param float timeout: Optional timeout in seconds.

        :return: A :class:`~cursor.CursorFactory` object.
        """
        self._check_open()
        return cursor.CursorFactory(self, query, None, args,
                                    prefetch, timeout)

    async def prepare(self, query, *, timeout=None):
        """Create a *prepared statement* for the specified query.

        :param str query: Text of the query to create a prepared statement for.
        :param float timeout: Optional timeout value in seconds.

        :return: A :class:`~prepared_stmt.PreparedStatement` instance.
        """
        self._check_open()
        stmt = await self._get_statement(query, timeout, named=True)
        return prepared_stmt.PreparedStatement(self, query, stmt)

    async def fetch(self, query, *args, timeout=None) -> list:
        """Run a query and return the results as a list of :class:`Record`.

        :param str query: Query text.
        :param args: Query arguments.
        :param float timeout: Optional timeout value in seconds.

        :return list: A list of :class:`Record` instances.
        """
        self._check_open()
        return await self._execute(query, args, 0, timeout)

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
        self._check_open()
        data = await self._execute(query, args, 1, timeout)
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
        self._check_open()
        data = await self._execute(query, args, 1, timeout)
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
        self._check_open()

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
        self._check_open()

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
        self._mark_stmts_as_closed()
        self._listeners = {}
        self._aborted = True
        await self._protocol.close()

    def terminate(self):
        """Terminate the connection without waiting for pending data."""
        self._mark_stmts_as_closed()
        self._listeners = {}
        self._aborted = True
        self._protocol.abort()

    async def reset(self):
        self._check_open()
        self._listeners.clear()
        reset_query = self._get_reset_query()
        if reset_query:
            await self.execute(reset_query)

    def _check_open(self):
        if self.is_closed():
            raise exceptions.InterfaceError('connection is closed')

    def _get_unique_id(self, prefix):
        self._uid += 1
        return '__asyncpg_{}_{}__'.format(prefix, self._uid)

    def _mark_stmts_as_closed(self):
        for stmt in self._stmt_cache.iter_statements():
            stmt.mark_closed()

        for stmt in self._stmts_to_close:
            stmt.mark_closed()

        self._stmt_cache.clear()
        self._stmts_to_close.clear()

    def _maybe_gc_stmt(self, stmt):
        if stmt.refs == 0 and not self._stmt_cache.has(stmt.query):
            # If low-level `stmt` isn't referenced from any high-level
            # `PreparedStatament` object and is not in the `_stmt_cache`:
            #
            #  * mark it as closed, which will make it non-usable
            #    for any `PreparedStatament` or for methods like
            #    `Connection.fetch()`.
            #
            # * schedule it to be formally closed on the server.
            stmt.mark_closed()
            self._stmts_to_close.add(stmt)

    async def _cleanup_stmts(self):
        # Called whenever we create a new prepared statement in
        # `Connection._get_statement()` and `_stmts_to_close` is
        # not empty.
        to_close = self._stmts_to_close
        self._stmts_to_close = set()
        for stmt in to_close:
            # It is imperative that statements are cleaned properly,
            # so we ignore the timeout.
            await self._protocol.close_statement(stmt, protocol.NO_TIMEOUT)

    def _cancel_current_command(self, waiter):
        async def cancel():
            try:
                # Open new connection to the server
                r, w = await connect_utils._open_connection(
                    loop=self._loop, addr=self._addr, params=self._params)
            except Exception as ex:
                waiter.set_exception(ex)
                return

            try:
                # Pack CancelRequest message
                msg = struct.pack('!llll', 16, 80877102,
                                  self._protocol.backend_pid,
                                  self._protocol.backend_secret)

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

        if self._proxy is None:
            con_ref = self
        else:
            # `_proxy` is not None when the connection is a member
            # of a connection pool.  Which means that the user is working
            # with a PooledConnectionProxy instance, and expects to see it
            # (and not the actual Connection) in their event callbacks.
            con_ref = self._proxy

        for cb in self._listeners[channel]:
            try:
                cb(con_ref, pid, channel, payload)
            except Exception as ex:
                self._loop.call_exception_handler({
                    'message': 'Unhandled exception in asyncpg notification '
                               'listener callback {!r}'.format(cb),
                    'exception': ex
                })

    def _get_reset_query(self):
        if self._reset_query is not None:
            return self._reset_query

        caps = self._server_caps

        _reset_query = []
        if self._protocol.is_in_transaction() or self._top_xact is not None:
            self._loop.call_exception_handler({
                'message': 'Resetting connection with an '
                           'active transaction {!r}'.format(self)
            })
            self._top_xact = None
            _reset_query.append('ROLLBACK;')
        if caps.advisory_locks:
            _reset_query.append('SELECT pg_advisory_unlock_all();')
        if caps.sql_close_all:
            _reset_query.append('CLOSE ALL;')
        if caps.notifications and caps.plpgsql:
            _reset_query.append('''
                DO $$
                BEGIN
                    PERFORM * FROM pg_listening_channels() LIMIT 1;
                    IF FOUND THEN
                        UNLISTEN *;
                    END IF;
                END;
                $$;
            ''')
        if caps.sql_reset:
            _reset_query.append('RESET ALL;')

        _reset_query = '\n'.join(_reset_query)
        self._reset_query = _reset_query

        return _reset_query

    def _set_proxy(self, proxy):
        if self._proxy is not None and proxy is not None:
            # Should not happen unless there is a bug in `Pool`.
            raise exceptions.InterfaceError(
                'internal asyncpg error: connection is already proxied')

        self._proxy = proxy

    def _drop_local_statement_cache(self):
        self._stmt_cache.clear()

    def _drop_global_statement_cache(self):
        if self._proxy is not None:
            # This connection is a member of a pool, so we delegate
            # the cache drop to the pool.
            pool = self._proxy._holder._pool
            pool._drop_statement_cache()
        else:
            self._drop_local_statement_cache()

    async def _execute(self, query, args, limit, timeout, return_status=False):
        executor = lambda stmt, timeout: self._protocol.bind_execute(
            stmt, args, '', limit, return_status, timeout)
        timeout = self._protocol._get_timeout(timeout)
        with self._stmt_exclusive_section:
            return await self._do_execute(query, executor, timeout)

    async def _executemany(self, query, args, timeout):
        executor = lambda stmt, timeout: self._protocol.bind_execute_many(
            stmt, args, '', timeout)
        timeout = self._protocol._get_timeout(timeout)
        with self._stmt_exclusive_section:
            return await self._do_execute(query, executor, timeout)

    async def _do_execute(self, query, executor, timeout, retry=True):
        if timeout is None:
            stmt = await self._get_statement(query, None)
        else:
            before = time.monotonic()
            stmt = await self._get_statement(query, timeout)
            after = time.monotonic()
            timeout -= after - before
            before = after

        try:
            if timeout is None:
                result = await executor(stmt, None)
            else:
                try:
                    result = await executor(stmt, timeout)
                finally:
                    after = time.monotonic()
                    timeout -= after - before

        except exceptions.InvalidCachedStatementError as e:
            # PostgreSQL will raise an exception when it detects
            # that the result type of the query has changed from
            # when the statement was prepared.  This may happen,
            # for example, after an ALTER TABLE or SET search_path.
            #
            # When this happens, and there is no transaction running,
            # we can simply re-prepare the statement and try once
            # again.  We deliberately retry only once as this is
            # supposed to be a rare occurrence.
            #
            # If the transaction _is_ running, this error will put it
            # into an error state, and we have no choice but to
            # re-raise the exception.
            #
            # In either case we clear the statement cache for this
            # connection and all other connections of the pool this
            # connection belongs to (if any).
            #
            # See https://github.com/MagicStack/asyncpg/issues/72
            # and https://github.com/MagicStack/asyncpg/issues/76
            # for discussion.
            #
            self._drop_global_statement_cache()
            if self._protocol.is_in_transaction() or not retry:
                raise
            else:
                result = await self._do_execute(
                    query, executor, timeout, retry=False)

        return result


async def connect(dsn=None, *,
                  host=None, port=None,
                  user=None, password=None,
                  database=None,
                  loop=None,
                  timeout=60,
                  statement_cache_size=100,
                  max_cached_statement_lifetime=300,
                  max_cacheable_statement_size=1024 * 15,
                  command_timeout=None,
                  ssl=None,
                  connection_class=Connection,
                  server_settings=None):
    r"""A coroutine to establish a connection to a PostgreSQL server.

    Returns a new :class:`~asyncpg.connection.Connection` object.

    :param dsn:
        Connection arguments specified using as a single string in the
        following format:
        ``postgres://user:pass@host:port/database?option=value``

    :param host:
        database host address or a path to the directory containing
        database server UNIX socket (defaults to the default UNIX socket,
        or the value of the ``PGHOST`` environment variable, if set).

    :param port:
        connection port number (defaults to ``5432``, or the value of
        the ``PGPORT`` environment variable, if set)

    :param user:
        the name of the database role used for authentication
        (defaults to the name of the effective user of the process
        making the connection, or the value of ``PGUSER`` environment
        variable, if set)

    :param database:
        the name of the database (defaults to the value of ``PGDATABASE``
        environment variable, if set.)

    :param password:
        password used for authentication

    :param loop:
        An asyncio event loop instance.  If ``None``, the default
        event loop will be used.

    :param float timeout:
        connection timeout in seconds.

    :param int statement_cache_size:
        the size of prepared statement LRU cache.  Pass ``0`` to
        disable the cache.

    :param int max_cached_statement_lifetime:
        the maximum time in seconds a prepared statement will stay
        in the cache.  Pass ``0`` to allow statements be cached
        indefinitely.

    :param int max_cacheable_statement_size:
        the maximum size of a statement that can be cached (15KiB by
        default).  Pass ``0`` to allow all statements to be cached
        regardless of their size.

    :param float command_timeout:
        the default timeout for operations on this connection
        (the default is ``None``: no timeout).

    :param ssl:
        pass ``True`` or an `ssl.SSLContext <SSLContext_>`_ instance to
        require an SSL connection.  If ``True``, a default SSL context
        returned by `ssl.create_default_context() <create_default_context_>`_
        will be used.

    :param dict server_settings:
        an optional dict of server parameters.

    :param Connection connection_class:
        class of the returned connection object.  Must be a subclass of
        :class:`~asyncpg.connection.Connection`.

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

    .. versionadded:: 0.10.0
       Added ``max_cached_statement_use_count`` parameter.

    .. versionchanged:: 0.11.0
       Removed ability to pass arbitrary keyword arguments to set
       server settings.  Added a dedicated parameter ``server_settings``
       for that.

    .. versionadded:: 0.11.0
       Added ``connection_class`` parameter.

    .. _SSLContext: https://docs.python.org/3/library/ssl.html#ssl.SSLContext
    .. _create_default_context: https://docs.python.org/3/library/ssl.html#\
                                ssl.create_default_context
    """
    if not issubclass(connection_class, Connection):
        raise TypeError(
            'connection_class is expected to be a subclass of '
            'asyncpg.Connection, got {!r}'.format(connection_class))

    if loop is None:
        loop = asyncio.get_event_loop()

    return await connect_utils._connect(
        loop=loop, timeout=timeout, connection_class=connection_class,
        dsn=dsn, host=host, port=port, user=user, password=password,
        ssl=ssl, database=database,
        server_settings=server_settings,
        command_timeout=command_timeout,
        statement_cache_size=statement_cache_size,
        max_cached_statement_lifetime=max_cached_statement_lifetime,
        max_cacheable_statement_size=max_cacheable_statement_size)


class _StatementCacheEntry:

    __slots__ = ('_query', '_statement', '_cache', '_cleanup_cb')

    def __init__(self, cache, query, statement):
        self._cache = cache
        self._query = query
        self._statement = statement
        self._cleanup_cb = None


class _StatementCache:

    __slots__ = ('_loop', '_entries', '_max_size', '_on_remove',
                 '_max_lifetime')

    def __init__(self, *, loop, max_size, on_remove, max_lifetime):
        self._loop = loop
        self._max_size = max_size
        self._on_remove = on_remove
        self._max_lifetime = max_lifetime

        # We use an OrderedDict for LRU implementation.  Operations:
        #
        # * We use a simple `__setitem__` to push a new entry:
        #       `entries[key] = new_entry`
        #   That will push `new_entry` to the *end* of the entries dict.
        #
        # * When we have a cache hit, we call
        #       `entries.move_to_end(key, last=True)`
        #   to move the entry to the *end* of the entries dict.
        #
        # * When we need to remove entries to maintain `max_size`, we call
        #       `entries.popitem(last=False)`
        #   to remove an entry from the *beginning* of the entries dict.
        #
        # So new entries and hits are always promoted to the end of the
        # entries dict, whereas the unused one will group in the
        # beginning of it.
        self._entries = collections.OrderedDict()

    def __len__(self):
        return len(self._entries)

    def get_max_size(self):
        return self._max_size

    def set_max_size(self, new_size):
        assert new_size >= 0
        self._max_size = new_size
        self._maybe_cleanup()

    def get_max_lifetime(self):
        return self._max_lifetime

    def set_max_lifetime(self, new_lifetime):
        assert new_lifetime >= 0
        self._max_lifetime = new_lifetime
        for entry in self._entries.values():
            # For every entry cancel the existing callback
            # and setup a new one if necessary.
            self._set_entry_timeout(entry)

    def get(self, query, *, promote=True):
        if not self._max_size:
            # The cache is disabled.
            return

        entry = self._entries.get(query)  # type: _StatementCacheEntry
        if entry is None:
            return

        if entry._statement.closed:
            # Happens in unittests when we call `stmt._state.mark_closed()`
            # manually.
            self._entries.pop(query)
            self._clear_entry_callback(entry)
            return

        if promote:
            # `promote` is `False` when `get()` is called by `has()`.
            self._entries.move_to_end(query, last=True)

        return entry._statement

    def has(self, query):
        return self.get(query, promote=False) is not None

    def put(self, query, statement):
        if not self._max_size:
            # The cache is disabled.
            return

        self._entries[query] = self._new_entry(query, statement)

        # Check if the cache is bigger than max_size and trim it
        # if necessary.
        self._maybe_cleanup()

    def iter_statements(self):
        return (e._statement for e in self._entries.values())

    def clear(self):
        # First, make sure that we cancel all scheduled callbacks.
        for entry in self._entries.values():
            self._clear_entry_callback(entry)

        # Clear the entries dict.
        self._entries.clear()

    def _set_entry_timeout(self, entry):
        # Clear the existing timeout.
        self._clear_entry_callback(entry)

        # Set the new timeout if it's not 0.
        if self._max_lifetime:
            entry._cleanup_cb = self._loop.call_later(
                self._max_lifetime, self._on_entry_expired, entry)

    def _new_entry(self, query, statement):
        entry = _StatementCacheEntry(self, query, statement)
        self._set_entry_timeout(entry)
        return entry

    def _on_entry_expired(self, entry):
        # `call_later` callback, called when an entry stayed longer
        # than `self._max_lifetime`.
        if self._entries.get(entry._query) is entry:
            self._entries.pop(entry._query)
            self._on_remove(entry._statement)

    def _clear_entry_callback(self, entry):
        if entry._cleanup_cb is not None:
            entry._cleanup_cb.cancel()

    def _maybe_cleanup(self):
        # Delete cache entries until the size of the cache is `max_size`.
        while len(self._entries) > self._max_size:
            old_query, old_entry = self._entries.popitem(last=False)
            self._clear_entry_callback(old_entry)

            # Let the connection know that the statement was removed
            # from the cache.
            self._on_remove(old_entry._statement)


class _Atomic:
    __slots__ = ('_acquired',)

    def __init__(self):
        self._acquired = 0

    def __enter__(self):
        if self._acquired:
            raise exceptions.InterfaceError(
                'cannot perform operation: another operation is in progress')
        self._acquired = 1

    def __exit__(self, t, e, tb):
        self._acquired = 0


class _ConnectionProxy:
    # Base class to enable `isinstance(Connection)` check.
    __slots__ = ()


ServerCapabilities = collections.namedtuple(
    'ServerCapabilities',
    ['advisory_locks', 'notifications', 'plpgsql', 'sql_reset',
     'sql_close_all'])
ServerCapabilities.__doc__ = 'PostgreSQL server capabilities.'


def _detect_server_capabilities(server_version, connection_settings):
    if hasattr(connection_settings, 'padb_revision'):
        # Amazon Redshift detected.
        advisory_locks = False
        notifications = False
        plpgsql = False
        sql_reset = True
        sql_close_all = False
    elif hasattr(connection_settings, 'crdb_version'):
        # CocroachDB detected.
        advisory_locks = False
        notifications = False
        plpgsql = False
        sql_reset = False
        sql_close_all = False
    else:
        # Standard PostgreSQL server assumed.
        advisory_locks = True
        notifications = True
        plpgsql = True
        sql_reset = True
        sql_close_all = True

    return ServerCapabilities(
        advisory_locks=advisory_locks,
        notifications=notifications,
        plpgsql=plpgsql,
        sql_reset=sql_reset,
        sql_close_all=sql_close_all
    )
