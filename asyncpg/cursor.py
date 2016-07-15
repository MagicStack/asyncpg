import collections

from . import compat
from . import exceptions


class CursorFactory:

    __slots__ = ('_state', '_connection', '_args', '_prefetch')

    def __init__(self, connection, state, args, prefetch):
        self._connection = connection
        self._state = state
        self._args = args
        self._prefetch = prefetch
        state.attach()

    @compat.aiter_compat
    def __aiter__(self):
        prefetch = 100 if self._prefetch is None else self._prefetch
        return CursorIterator(self._connection, self._state,
                              self._args, prefetch)

    def __await__(self):
        if self._prefetch is not None:
            raise exceptions.InterfaceError(
                'prefetch argument can only be specified for iterable cursor')
        cursor = Cursor(self._connection, self._state, self._args)
        return cursor._init().__await__()

    def __del__(self):
        if self._state is not None:
            self._state.detach()
            self._connection._maybe_gc_stmt(self._state)


class BaseCursor:

    __slots__ = ('_state', '_connection', '_args', '_portal_name',
                 '_exhausted')

    def __init__(self, connection, state, args):
        self._args = args
        self._connection = connection
        self._state = state
        state.attach()
        self._portal_name = None
        self._exhausted = False

    def _check_ready(self):
        if self._state.closed:
            raise exceptions.InterfaceError(
                'cursor: the prepared statement is closed')

        if not self._connection._top_xact:
            raise exceptions.NoActiveSQLTransactionError(
                'cursor cannot be created outside of a transaction')

    async def _bind_exec(self, n):
        self._check_ready()

        if self._portal_name:
            raise exceptions.InterfaceError(
                'cursor already has an open portal')

        con = self._connection
        protocol = con._protocol

        self._portal_name = con._request_portal_name()
        buffer = await protocol.bind_execute(self._state, self._args,
                                             self._portal_name, n)
        self._exhausted = self._state.last_exec_completed
        return buffer

    async def _bind(self):
        self._check_ready()

        if self._portal_name:
            raise exceptions.InterfaceError(
                'cursor already has an open portal')

        con = self._connection
        protocol = con._protocol

        self._portal_name = con._request_portal_name()
        buffer = await protocol.bind(self._state, self._args,
                                     self._portal_name)
        return buffer

    async def _exec(self, n):
        self._check_ready()

        if not self._portal_name:
            raise exceptions.InterfaceError(
                'cursor does not have an open portal')

        protocol = self._connection._protocol
        buffer = await protocol.execute(self._state, self._portal_name, n)
        self._exhausted = self._state.last_exec_completed
        return buffer

    def __repr__(self):
        attrs = []
        if self._exhausted:
            attrs.append('exhausted')
        attrs.append('')  # to separate from id

        if self.__class__.__module__.startswith('asyncpg.'):
            mod = 'asyncpg'
        else:
            mod = self.__class__.__module__

        return '<{}.{} "{!s:.30}" {}{:#x}>'.format(
            mod, self.__class__.__name__,
            self._state.query,
            ' '.join(attrs), id(self))

    def __del__(self):
        if self._state is not None:
            self._state.detach()
            self._connection._maybe_gc_stmt(self._state)


class CursorIterator(BaseCursor):

    __slots__ = ('_buffer', '_prefetch')

    def __init__(self, connection, state, args, prefetch):
        super().__init__(connection, state, args)

        if prefetch <= 0:
            raise exceptions.InterfaceError(
                'prefetch argument must be greater than zero')

        self._buffer = collections.deque()
        self._prefetch = prefetch

    @compat.aiter_compat
    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._portal_name:
            buffer = await self._bind_exec(self._prefetch)
            self._buffer.extend(buffer)

        if not self._buffer and not self._exhausted:
            buffer = await self._exec(self._prefetch)
            self._buffer.extend(buffer)

        if self._buffer:
            return self._buffer.popleft()

        raise StopAsyncIteration


class Cursor(BaseCursor):

    __slots__ = ()

    async def _init(self):
        self._check_ready()
        await self._bind()
        return self

    async def fetch(self, n):
        if n <= 0:
            raise exceptions.InterfaceError('n must be greater than zero')
        if self._exhausted:
            return []
        recs = await self._exec(n)
        if len(recs) < n:
            self._exhausted = True
        return recs

    async def fetchrow(self):
        if self._exhausted:
            return None
        recs = await self._exec(1)
        if len(recs) < 1:
            self._exhausted = True
            return None
        return recs[0]

    async def forward(self, n):
        if n <= 0:
            raise exceptions.InterfaceError('n must be greater than zero')

        st = await self._connection.prepare(
            'MOVE FORWARD {:d} {}'.format(n, self._portal_name))
        await st.fetch()

        moved = st.get_statusmsg()
        advanced = int(moved.split()[1])
        if advanced < n:
            self._exhausted = True

        return advanced
