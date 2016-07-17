# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio

from . import connection
from . import exceptions


class Pool:

    __slots__ = ('_queue', '_loop', '_minsize', '_maxsize',
                 '_connect_args', '_connect_kwargs',
                 '_working_addr', '_working_opts',
                 '_con_count', '_max_queries', '_connections',
                 '_initialized')

    def __init__(self, *connect_args,
                 min_size=10,
                 max_size=10,
                 max_queries=50000,
                 loop=None,
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

        self._connect_args = connect_args
        self._connect_kwargs = connect_kwargs

        self._working_addr = None
        self._working_opts = None

        self._reset()

    async def _new_connection(self, timeout=None):
        if self._working_addr is None:
            con = await connection.connect(*self._connect_args,
                                           timeout=timeout,
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

            con = await connection.connect(host=host, port=port,
                                           timeout=timeout,
                                           loop=self._loop,
                                           **self._working_opts)

        self._connections.add(con)
        return con

    async def _init(self):
        if self._initialized:
            return

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

    async def acquire(self, *, timeout=None):
        self._check_init()

        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            pass

        if self._con_count < self._maxsize:
            self._con_count += 1
            try:
                con = await self._new_connection(timeout=timeout)
            except:
                self._con_count -= 1
                raise
            return con

        if timeout is None:
            return await self._queue.get()
        else:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout,
                                          loop=self._loop)

    async def release(self, connection):
        self._check_init()
        if connection._protocol.queries_count >= self._max_queries:
            self._con_count -= 1
            self._connections.remove(connection)
            await connection.close()
        else:
            await connection.reset()
            self._queue.put_nowait(connection)

    async def close(self):
        self._check_init()
        coros = []
        for con in self._connections:
            coros.append(con.close())
        await asyncio.gather(*coros, loop=self._loop)
        self._reset()

    def terminate(self):
        self._check_init()
        for con in self._connections:
            con.terminate()
        self._reset()

    def _check_init(self):
        if not self._initialized:
            raise exceptions.InterfaceError('pool is not initialized')

    def _reset(self):
        self._connections = set()
        self._con_count = 0
        self._initialized = False
        self._queue = asyncio.Queue(maxsize=self._maxsize, loop=self._loop)

    def __await__(self):
        return self._init().__await__()

    async def __aenter__(self):
        await self._init()
        return self

    async def __aexit__(self, *exc):
        await self.close()


def create_pool(*args, **kwargs):
    return Pool(*args, **kwargs)
