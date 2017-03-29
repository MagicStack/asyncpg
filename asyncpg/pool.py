# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import functools

from . import connection
from . import exceptions


class PooledConnectionProxyMeta(type):

    def __new__(mcls, name, bases, dct, *, wrap=False):
        if wrap:
            def get_wrapper(methname):
                meth = getattr(connection.Connection, methname)

                def wrapper(self, *args, **kwargs):
                    return self._dispatch(meth, args, kwargs)

                return wrapper

            for attrname in dir(connection.Connection):
                if attrname.startswith('_') or attrname in dct:
                    continue
                wrapper = get_wrapper(attrname)
                wrapper = functools.update_wrapper(
                    wrapper, getattr(connection.Connection, attrname))
                dct[attrname] = wrapper

            if '__doc__' not in dct:
                dct['__doc__'] = connection.Connection.__doc__

        return super().__new__(mcls, name, bases, dct)

    def __init__(cls, name, bases, dct, *, wrap=False):
        # Needed for Python 3.5 to handle `wrap` class keyword argument.
        super().__init__(name, bases, dct)


class PooledConnectionProxy(connection._ConnectionProxy,
                            metaclass=PooledConnectionProxyMeta,
                            wrap=True):

    __slots__ = ('_con', '_owner')

    def __init__(self, owner: 'Pool', con: connection.Connection):
        self._con = con
        self._owner = owner
        con._set_proxy(self)

    def _unwrap(self) -> connection.Connection:
        if self._con is None:
            raise exceptions.InterfaceError(
                'internal asyncpg error: cannot unwrap pooled connection')

        con, self._con = self._con, None
        con._set_proxy(None)
        return con

    def _dispatch(self, meth, args, kwargs):
        if self._con is None:
            raise exceptions.InterfaceError(
                'cannot call Connection.{}(): '
                'connection has been released back to the pool'.format(
                    meth.__name__))

        return meth(self._con, *args, **kwargs)

    def __repr__(self):
        if self._con is None:
            return '<{classname} [released] {id:#x}>'.format(
                classname=self.__class__.__name__, id=id(self))
        else:
            return '<{classname} {con!r} {id:#x}>'.format(
                classname=self.__class__.__name__, con=self._con, id=id(self))


class Pool:
    """A connection pool.

    Connection pool can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool.  Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Pools are created by calling :func:`~asyncpg.pool.create_pool`.
    """

    __slots__ = ('_queue', '_loop', '_minsize', '_maxsize',
                 '_connect_args', '_connect_kwargs',
                 '_working_addr', '_working_opts',
                 '_con_count', '_max_queries', '_connections',
                 '_initialized', '_closed', '_setup', '_init')

    def __init__(self, *connect_args,
                 min_size,
                 max_size,
                 max_queries,
                 setup,
                 init,
                 loop,
                 **connect_kwargs):

        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop

        if max_size <= 0:
            raise ValueError('max_size is expected to be greater than zero')

        if min_size <= 0:
            raise ValueError('min_size is expected to be greater than zero')

        if min_size > max_size:
            raise ValueError('min_size is greater than max_size')

        if max_queries <= 0:
            raise ValueError('max_queries is expected to be greater than zero')

        self._minsize = min_size
        self._maxsize = max_size
        self._max_queries = max_queries

        self._setup = setup
        self._init = init

        self._connect_args = connect_args
        self._connect_kwargs = connect_kwargs

        self._working_addr = None
        self._working_opts = None

        self._reset()

        self._closed = False

    async def _connect(self, *args, **kwargs):
        return await connection.connect(*args, **kwargs)

    async def _new_connection(self):
        if self._working_addr is None:
            con = await self._connect(*self._connect_args,
                                      loop=self._loop,
                                      **self._connect_kwargs)
            self._working_addr = con._addr
            self._working_opts = con._opts

        else:
            if isinstance(self._working_addr, str):
                host = self._working_addr
                port = 0
            else:
                host, port = self._working_addr

            con = await self._connect(host=host, port=port,
                                      loop=self._loop,
                                      **self._working_opts)

        if self._init is not None:
            await self._init(con)

        self._connections.add(con)
        return con

    async def _initialize(self):
        if self._initialized:
            return
        if self._closed:
            raise exceptions.InterfaceError('pool is closed')

        for _ in range(self._minsize):
            self._con_count += 1
            try:
                con = await self._new_connection()
            except:
                self._con_count -= 1
                raise
            self._queue.put_nowait(con)

        self._initialized = True
        return self

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
        if timeout is None:
            return await self._acquire_impl()
        else:
            return await asyncio.wait_for(self._acquire_impl(),
                                          timeout=timeout,
                                          loop=self._loop)

    async def _acquire_impl(self):
        self._check_init()

        try:
            con = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            con = None

        if con is None:
            if self._con_count < self._maxsize:
                self._con_count += 1
                try:
                    con = await self._new_connection()
                except:
                    self._con_count -= 1
                    raise
            else:
                con = await self._queue.get()

        con = PooledConnectionProxy(self, con)

        if self._setup is not None:
            try:
                await self._setup(con)
            except:
                await self.release(con)
                raise

        return con

    async def release(self, connection):
        """Release a database connection back to the pool."""

        if (connection.__class__ is not PooledConnectionProxy or
                connection._owner is not self):
            raise exceptions.InterfaceError(
                'Pool.release() received invalid connection: '
                '{connection!r} is not a member of this pool'.format(
                    connection=connection))

        if connection._con is None:
            # Already released, do nothing.
            return

        connection = connection._unwrap()

        # Use asyncio.shield() to guarantee that task cancellation
        # does not prevent the connection from being returned to the
        # pool properly.
        return await asyncio.shield(self._release_impl(connection),
                                    loop=self._loop)

    async def _release_impl(self, connection):
        self._check_init()
        if connection.is_closed():
            self._con_count -= 1
            self._connections.remove(connection)
        elif connection._protocol.queries_count >= self._max_queries:
            self._con_count -= 1
            self._connections.remove(connection)
            await connection.close()
        else:
            await connection.reset()
            self._queue.put_nowait(connection)

    async def close(self):
        """Gracefully close all connections in the pool."""
        if self._closed:
            return
        self._check_init()
        self._closed = True
        coros = []
        for con in self._connections:
            coros.append(con.close())
        await asyncio.gather(*coros, loop=self._loop)
        self._reset()

    def terminate(self):
        """Terminate all connections in the pool."""
        if self._closed:
            return
        self._check_init()
        self._closed = True
        for con in self._connections:
            con.terminate()
        self._reset()

    def _check_init(self):
        if not self._initialized:
            raise exceptions.InterfaceError('pool is not initialized')
        if self._closed:
            raise exceptions.InterfaceError('pool is closed')

    def _reset(self):
        self._connections = set()
        self._con_count = 0
        self._initialized = False
        self._queue = asyncio.Queue(maxsize=self._maxsize, loop=self._loop)

    def __await__(self):
        return self._initialize().__await__()

    async def __aenter__(self):
        await self._initialize()
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
                setup=None,
                init=None,
                loop=None,
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

    :param str dsn: Connection arguments specified using as a single string in
                    the following format:
                    ``postgres://user:pass@host:port/database?option=value``.

    :param \*\*connect_kwargs: Keyword arguments for the
                               :func:`~asyncpg.connection.connect` function.
    :param int min_size: Number of connection the pool will be initialized
                         with.
    :param int max_size: Max number of connections in the pool.
    :param int max_queries: Number of queries after a connection is closed
                            and replaced with a new connection.
    :param coroutine setup: A coroutine to prepare a connection right before
                            it is returned from :meth:`~pool.Pool.acquire`.
                            An example use case would be to automatically
                            set up notifications listeners for all connections
                            of a pool.
    :param coroutine init:  A coroutine to initialize a connection when it
                            is created. An example use case would be to setup
                            type codecs with
                            :meth:`~asyncpg.connection.Connection.\
                            set_builtin_type_codec` or :meth:`~asyncpg.\
                            connection.Connection.set_type_codec`.
    :param loop: An asyncio event loop instance.  If ``None``, the default
                 event loop will be used.
    :return: An instance of :class:`~asyncpg.pool.Pool`.

    .. versionchanged:: 0.10.0
       An :exc:`~asyncpg.exceptions.InterfaceError` will be raised on any
       attempted operation on a released connection.
    """
    return Pool(dsn,
                min_size=min_size, max_size=max_size,
                max_queries=max_queries, loop=loop, setup=setup, init=init,
                **connect_kwargs)
