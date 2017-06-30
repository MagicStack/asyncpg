# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import functools
import inspect

from . import connection
from . import connect_utils
from . import exceptions


class PoolConnectionProxyMeta(type):

    def __new__(mcls, name, bases, dct, *, wrap=False):
        if wrap:
            for attrname in dir(connection.Connection):
                if attrname.startswith('_') or attrname in dct:
                    continue

                meth = getattr(connection.Connection, attrname)
                if not inspect.isfunction(meth):
                    continue

                wrapper = mcls._wrap_connection_method(attrname)
                wrapper = functools.update_wrapper(wrapper, meth)
                dct[attrname] = wrapper

            if '__doc__' not in dct:
                dct['__doc__'] = connection.Connection.__doc__

        return super().__new__(mcls, name, bases, dct)

    def __init__(cls, name, bases, dct, *, wrap=False):
        # Needed for Python 3.5 to handle `wrap` class keyword argument.
        super().__init__(name, bases, dct)

    @staticmethod
    def _wrap_connection_method(meth_name):
        def call_con_method(self, *args, **kwargs):
            # This method will be owned by PoolConnectionProxy class.
            if self._con is None:
                raise exceptions.InterfaceError(
                    'cannot call Connection.{}(): '
                    'connection has been released back to the pool'.format(
                        meth_name))

            meth = getattr(self._con.__class__, meth_name)
            return meth(self._con, *args, **kwargs)

        return call_con_method


class PoolConnectionProxy(connection._ConnectionProxy,
                          metaclass=PoolConnectionProxyMeta,
                          wrap=True):

    __slots__ = ('_con', '_holder')

    def __init__(self, holder: 'PoolConnectionHolder',
                 con: connection.Connection):
        self._con = con
        self._holder = holder
        con._set_proxy(self)

    def __getattr__(self, attr):
        # Proxy all unresolved attributes to the wrapped Connection object.
        return getattr(self._con, attr)

    def _detach(self):
        if self._con is None:
            raise exceptions.InterfaceError(
                'cannot detach PoolConnectionProxy: already detached')

        con, self._con = self._con, None
        con._set_proxy(None)

    def __repr__(self):
        if self._con is None:
            return '<{classname} [released] {id:#x}>'.format(
                classname=self.__class__.__name__, id=id(self))
        else:
            return '<{classname} {con!r} {id:#x}>'.format(
                classname=self.__class__.__name__, con=self._con, id=id(self))


class PoolConnectionHolder:

    __slots__ = ('_con', '_pool', '_loop',
                 '_connect_args', '_connect_kwargs',
                 '_max_queries', '_setup', '_init',
                 '_max_inactive_time', '_in_use',
                 '_inactive_callback')

    def __init__(self, pool, *, connect_args, connect_kwargs,
                 max_queries, setup, init, max_inactive_time):

        self._pool = pool
        self._con = None

        self._connect_args = connect_args
        self._connect_kwargs = connect_kwargs
        self._max_queries = max_queries
        self._max_inactive_time = max_inactive_time
        self._setup = setup
        self._init = init
        self._inactive_callback = None
        self._in_use = False

    async def connect(self):
        assert self._con is None

        if self._pool._working_addr is None:
            # First connection attempt on this pool.
            con = await connection.connect(
                *self._connect_args,
                loop=self._pool._loop,
                connection_class=self._pool._connection_class,
                **self._connect_kwargs)

            self._pool._working_addr = con._addr
            self._pool._working_config = con._config
            self._pool._working_params = con._params

        else:
            # We've connected before and have a resolved address,
            # and parsed options and config.
            con = await connect_utils._connect_addr(
                loop=self._pool._loop,
                addr=self._pool._working_addr,
                timeout=self._pool._working_params.connect_timeout,
                config=self._pool._working_config,
                params=self._pool._working_params,
                connection_class=self._pool._connection_class)

        if self._init is not None:
            await self._init(con)

        self._con = con

    async def acquire(self) -> PoolConnectionProxy:
        if self._con is None:
            await self.connect()

        self._maybe_cancel_inactive_callback()

        proxy = PoolConnectionProxy(self, self._con)

        if self._setup is not None:
            try:
                await self._setup(proxy)
            except Exception as ex:
                # If a user-defined `setup` function fails, we don't
                # know if the connection is safe for re-use, hence
                # we close it.  A new connection will be created
                # when `acquire` is called again.
                try:
                    proxy._detach()
                    # Use `close` to close the connection gracefully.
                    # An exception in `setup` isn't necessarily caused
                    # by an IO or a protocol error.
                    await self._con.close()
                finally:
                    self._con = None
                    raise ex

        self._in_use = True
        return proxy

    async def release(self):
        assert self._in_use
        self._in_use = False

        if self._con.is_closed():
            self._con = None

        elif self._con._protocol.queries_count >= self._max_queries:
            try:
                await self._con.close()
            finally:
                self._con = None

        else:
            try:
                await self._con.reset()
            except Exception as ex:
                # If the `reset` call failed, terminate the connection.
                # A new one will be created when `acquire` is called
                # again.
                try:
                    # An exception in `reset` is most likely caused by
                    # an IO error, so terminate the connection.
                    self._con.terminate()
                finally:
                    self._con = None
                    raise ex

        assert self._inactive_callback is None
        if self._max_inactive_time and self._con is not None:
            self._inactive_callback = self._pool._loop.call_later(
                self._max_inactive_time, self._deactivate_connection)

    async def close(self):
        self._maybe_cancel_inactive_callback()
        if self._con is None:
            return
        if self._con.is_closed():
            self._con = None
            return

        try:
            await self._con.close()
        finally:
            self._con = None

    def terminate(self):
        self._maybe_cancel_inactive_callback()
        if self._con is None:
            return
        if self._con.is_closed():
            self._con = None
            return

        try:
            self._con.terminate()
        finally:
            self._con = None

    def _maybe_cancel_inactive_callback(self):
        if self._inactive_callback is not None:
            self._inactive_callback.cancel()
            self._inactive_callback = None

    def _deactivate_connection(self):
        assert not self._in_use
        if self._con is None or self._con.is_closed():
            return
        self._con.terminate()
        self._con = None


class Pool:
    """A connection pool.

    Connection pool can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool.  Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Pools are created by calling :func:`~asyncpg.pool.create_pool`.
    """

    __slots__ = ('_queue', '_loop', '_minsize', '_maxsize',
                 '_working_addr', '_working_config', '_working_params',
                 '_holders', '_initialized', '_closed',
                 '_connection_class')

    def __init__(self, *connect_args,
                 min_size,
                 max_size,
                 max_queries,
                 max_inactive_connection_lifetime,
                 setup,
                 init,
                 loop,
                 connection_class,
                 **connect_kwargs):

        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop

        if max_size <= 0:
            raise ValueError('max_size is expected to be greater than zero')

        if min_size < 0:
            raise ValueError(
                'min_size is expected to be greater or equal to zero')

        if min_size > max_size:
            raise ValueError('min_size is greater than max_size')

        if max_queries <= 0:
            raise ValueError('max_queries is expected to be greater than zero')

        if max_inactive_connection_lifetime < 0:
            raise ValueError(
                'max_inactive_connection_lifetime is expected to be greater '
                'or equal to zero')

        self._minsize = min_size
        self._maxsize = max_size

        self._holders = []
        self._initialized = False
        self._queue = asyncio.LifoQueue(maxsize=self._maxsize, loop=self._loop)

        self._working_addr = None
        self._working_config = None
        self._working_params = None

        self._connection_class = connection_class

        self._closed = False

        for _ in range(max_size):
            ch = PoolConnectionHolder(
                self,
                connect_args=connect_args,
                connect_kwargs=connect_kwargs,
                max_queries=max_queries,
                max_inactive_time=max_inactive_connection_lifetime,
                setup=setup,
                init=init)

            self._holders.append(ch)
            self._queue.put_nowait(ch)

    async def _async__init__(self):
        if self._initialized:
            return
        if self._closed:
            raise exceptions.InterfaceError('pool is closed')

        if self._minsize:
            # Since we use a LIFO queue, the first items in the queue will be
            # the last ones in `self._holders`.  We want to pre-connect the
            # first few connections in the queue, therefore we want to walk
            # `self._holders` in reverse.

            # Connect the first connection holder in the queue so that it
            # can record `_working_addr` and `_working_opts`, which will
            # speed up successive connection attempts.
            first_ch = self._holders[-1]  # type: PoolConnectionHolder
            await first_ch.connect()

            if self._minsize > 1:
                connect_tasks = []
                for i, ch in enumerate(reversed(self._holders[:-1])):
                    # `minsize - 1` because we already have first_ch
                    if i >= self._minsize - 1:
                        break
                    connect_tasks.append(ch.connect())

                await asyncio.gather(*connect_tasks, loop=self._loop)

        self._initialized = True
        return self

    async def execute(self, query: str, *args, timeout: float=None) -> str:
        """Execute an SQL command (or commands).

        Pool performs this operation using one of its connections.  Other than
        that, it behaves identically to
        :meth:`Connection.execute() <connection.Connection.execute>`.

        .. versionadded:: 0.10.0
        """
        async with self.acquire() as con:
            return await con.execute(query, *args, timeout=timeout)

    async def executemany(self, command: str, args, *, timeout: float=None):
        """Execute an SQL *command* for each sequence of arguments in *args*.

        Pool performs this operation using one of its connections.  Other than
        that, it behaves identically to
        :meth:`Connection.executemany() <connection.Connection.executemany>`.

        .. versionadded:: 0.10.0
        """
        async with self.acquire() as con:
            return await con.executemany(command, args, timeout=timeout)

    async def fetch(self, query, *args, timeout=None) -> list:
        """Run a query and return the results as a list of :class:`Record`.

        Pool performs this operation using one of its connections.  Other than
        that, it behaves identically to
        :meth:`Connection.fetch() <connection.Connection.fetch>`.

        .. versionadded:: 0.10.0
        """
        async with self.acquire() as con:
            return await con.fetch(query, *args, timeout=timeout)

    async def fetchval(self, query, *args, column=0, timeout=None):
        """Run a query and return a value in the first row.

        Pool performs this operation using one of its connections.  Other than
        that, it behaves identically to
        :meth:`Connection.fetchval() <connection.Connection.fetchval>`.

        .. versionadded:: 0.10.0
        """
        async with self.acquire() as con:
            return await con.fetchval(
                query, *args, column=column, timeout=timeout)

    async def fetchrow(self, query, *args, timeout=None):
        """Run a query and return the first row.

        Pool performs this operation using one of its connections.  Other than
        that, it behaves identically to
        :meth:`Connection.fetchrow() <connection.Connection.fetchrow>`.

        .. versionadded:: 0.10.0
        """
        async with self.acquire() as con:
            return await con.fetchrow(query, *args, timeout=timeout)

    def acquire(self, *, timeout=None):
        """Acquire a database connection from the pool.

        :param float timeout: A timeout for acquiring a Connection.
        :return: An instance of :class:`~asyncpg.connection.Connection`.

        Can be used in an ``await`` expression or with an ``async with`` block.

        .. code-block:: python

            async with pool.acquire() as con:
                await con.execute(...)

        Or:

        .. code-block:: python

            con = await pool.acquire()
            try:
                await con.execute(...)
            finally:
                await pool.release(con)
        """
        return PoolAcquireContext(self, timeout)

    async def _acquire(self, timeout):
        async def _acquire_impl():
            ch = await self._queue.get()  # type: PoolConnectionHolder
            try:
                proxy = await ch.acquire()  # type: PoolConnectionProxy
            except Exception:
                self._queue.put_nowait(ch)
                raise
            else:
                return proxy

        self._check_init()
        if timeout is None:
            return await _acquire_impl()
        else:
            return await asyncio.wait_for(
                _acquire_impl(), timeout=timeout, loop=self._loop)

    async def release(self, connection):
        """Release a database connection back to the pool."""
        async def _release_impl(ch: PoolConnectionHolder):
            try:
                await ch.release()
            finally:
                self._queue.put_nowait(ch)

        self._check_init()

        if (type(connection) is not PoolConnectionProxy or
                connection._holder._pool is not self):
            raise exceptions.InterfaceError(
                'Pool.release() received invalid connection: '
                '{connection!r} is not a member of this pool'.format(
                    connection=connection))

        if connection._con is None:
            # Already released, do nothing.
            return

        connection._detach()

        # Use asyncio.shield() to guarantee that task cancellation
        # does not prevent the connection from being returned to the
        # pool properly.
        return await asyncio.shield(_release_impl(connection._holder),
                                    loop=self._loop)

    async def close(self):
        """Gracefully close all connections in the pool."""
        if self._closed:
            return
        self._check_init()
        self._closed = True
        coros = [ch.close() for ch in self._holders]
        await asyncio.gather(*coros, loop=self._loop)

    def terminate(self):
        """Terminate all connections in the pool."""
        if self._closed:
            return
        self._check_init()
        self._closed = True
        for ch in self._holders:
            ch.terminate()

    def _check_init(self):
        if not self._initialized:
            raise exceptions.InterfaceError('pool is not initialized')
        if self._closed:
            raise exceptions.InterfaceError('pool is closed')

    def _drop_statement_cache(self):
        # Drop statement cache for all connections in the pool.
        for ch in self._holders:
            if ch._con is not None:
                ch._con._drop_local_statement_cache()

    def __await__(self):
        return self._async__init__().__await__()

    async def __aenter__(self):
        await self._async__init__()
        return self

    async def __aexit__(self, *exc):
        await self.close()


class PoolAcquireContext:

    __slots__ = ('timeout', 'connection', 'done', 'pool')

    def __init__(self, pool, timeout):
        self.pool = pool
        self.timeout = timeout
        self.connection = None
        self.done = False

    async def __aenter__(self):
        if self.connection is not None or self.done:
            raise exceptions.InterfaceError('a connection is already acquired')
        self.connection = await self.pool._acquire(self.timeout)
        return self.connection

    async def __aexit__(self, *exc):
        self.done = True
        con = self.connection
        self.connection = None
        await self.pool.release(con)

    def __await__(self):
        self.done = True
        return self.pool._acquire(self.timeout).__await__()


def create_pool(dsn=None, *,
                min_size=10,
                max_size=10,
                max_queries=50000,
                max_inactive_connection_lifetime=300.0,
                setup=None,
                init=None,
                loop=None,
                connection_class=connection.Connection,
                **connect_kwargs):
    r"""Create a connection pool.

    Can be used either with an ``async with`` block:

    .. code-block:: python

        async with asyncpg.create_pool(user='postgres',
                                       command_timeout=60) as pool:
            async with pool.acquire() as con:
                await con.fetch('SELECT 1')

    Or directly with ``await``:

    .. code-block:: python

        pool = await asyncpg.create_pool(user='postgres', command_timeout=60)
        con = await pool.acquire()
        try:
            await con.fetch('SELECT 1')
        finally:
            await pool.release(con)

    :param str dsn:
        Connection arguments specified using as a single string in
        the following format:
        ``postgres://user:pass@host:port/database?option=value``.

    :param \*\*connect_kwargs:
        Keyword arguments for the :func:`~asyncpg.connection.connect`
        function.

    :param int min_size:
        Number of connection the pool will be initialized with.

    :param int max_size:
        Max number of connections in the pool.

    :param int max_queries:
        Number of queries after a connection is closed and replaced
        with a new connection.

    :param float max_inactive_connection_lifetime:
        Number of seconds after which inactive connections in the
        pool will be closed.  Pass ``0`` to disable this mechanism.

    :param coroutine setup:
        A coroutine to prepare a connection right before it is returned
        from :meth:`Pool.acquire() <pool.Pool.acquire>`.  An example use
        case would be to automatically set up notifications listeners for
        all connections of a pool.

    :param coroutine init:
        A coroutine to initialize a connection when it is created.
        An example use case would be to setup type codecs with
        :meth:`Connection.set_builtin_type_codec() <\
        asyncpg.connection.Connection.set_builtin_type_codec>`
        or :meth:`Connection.set_type_codec() <\
        asyncpg.connection.Connection.set_type_codec>`.

    :param loop:
        An asyncio event loop instance.  If ``None``, the default
        event loop will be used.

    :return: An instance of :class:`~asyncpg.pool.Pool`.

    .. versionchanged:: 0.10.0
       An :exc:`~asyncpg.exceptions.InterfaceError` will be raised on any
       attempted operation on a released connection.
    """
    if not issubclass(connection_class, connection.Connection):
        raise TypeError(
            'connection_class is expected to be a subclass of '
            'asyncpg.Connection, got {!r}'.format(connection_class))

    return Pool(
        dsn,
        connection_class=connection_class,
        min_size=min_size, max_size=max_size,
        max_queries=max_queries, loop=loop, setup=setup, init=init,
        max_inactive_connection_lifetime=max_inactive_connection_lifetime,
        **connect_kwargs)
