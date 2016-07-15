import collections

from . import compat
from . import exceptions


class Cursor:

    __slots__ = ('_state', '_connection', '_args', '_portal_name',
                 '_pos', '_buffer', '_exhausted', '_limit', '_iter')

    def __init__(self, connection, state, args, prefetch):
        self._connection = connection
        self._state = state
        state.attach()

        if prefetch <= 0:
            raise exceptions.InterfaceError(
                'prefetch argument must be greater than zero')

        self._args = args

        self._portal_name = None
        self._pos = 0
        self._buffer = collections.deque()
        self._exhausted = False

        self._iter = False
        self._limit = prefetch

    async def __init_iter(self):
        assert self._iter
        assert not self._portal_name
        self.__check_open()

        if not self._connection._top_xact:
            raise exceptions.NoActiveSQLTransactionError(
                'cursor cannot be iterated outside of a transaction')

        con = self._connection
        protocol = con._protocol

        self._portal_name = con._request_portal_name()

        buffer = await protocol.bind_execute(self._state, self._args,
                                             self._portal_name, self._limit)
        self._exhausted = self._state.last_exec_completed

        self._buffer.extend(buffer)

    async def __fetch_more(self):
        con = self._connection
        protocol = con._protocol
        self.__check_open()

        buffer = await protocol.execute(self._state, self._portal_name,
                                        self._limit)
        self._exhausted = self._state.last_exec_completed

        self._buffer.extend(buffer)

    def __check_open(self):
        if self._state.closed:
            raise exceptions.InterfaceError('prepared statement is closed')

    @compat.aiter_compat
    def __aiter__(self):
        self._iter = True
        return self

    async def __anext__(self):
        if not self._portal_name:
            await self.__init_iter()

        if not self._buffer and not self._exhausted:
            await self.__fetch_more()

        if self._buffer:
            self._pos += 1
            return self._buffer.popleft()

        raise StopAsyncIteration

    def __del__(self):
        if self._state is not None:
            self._state.detach()
            self._connection._maybe_gc_stmt(self._state)
