# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import collections
import typing

from . import connresource
from . import exceptions

if typing.TYPE_CHECKING:
    import sys

    if sys.version_info < (3, 11):
        from typing_extensions import Self
    else:
        from typing import Self

    from .protocol import protocol as _cprotocol
    from . import connection as _connection
    from . import compat

_RecordT = typing.TypeVar('_RecordT', bound='_cprotocol.Record')


class CursorFactory(connresource.ConnectionResource, typing.Generic[_RecordT]):
    """A cursor interface for the results of a query.

    A cursor interface can be used to initiate efficient traversal of the
    results of a large query.
    """

    __slots__ = (
        '_state',
        '_args',
        '_prefetch',
        '_query',
        '_timeout',
        '_record_class',
    )

    _state: _cprotocol.PreparedStatementState[_RecordT] | None
    _args: compat.Sequence[object]
    _prefetch: int | None
    _query: str
    _timeout: float | None
    _record_class: type[_RecordT] | None

    @typing.overload
    def __init__(
        self: CursorFactory[_RecordT],
        connection: _connection.Connection[_RecordT],
        query: str,
        state: _cprotocol.PreparedStatementState[_RecordT] | None,
        args: compat.Sequence[object],
        prefetch: int | None,
        timeout: float | None,
        record_class: None
    ) -> None:
        ...

    @typing.overload
    def __init__(
        self: CursorFactory[_RecordT],
        connection: _connection.Connection[typing.Any],
        query: str,
        state: _cprotocol.PreparedStatementState[_RecordT] | None,
        args: compat.Sequence[object],
        prefetch: int | None,
        timeout: float | None,
        record_class: type[_RecordT]
    ) -> None:
        ...

    def __init__(
        self,
        connection: _connection.Connection[typing.Any],
        query: str,
        state: _cprotocol.PreparedStatementState[_RecordT] | None,
        args: compat.Sequence[object],
        prefetch: int | None,
        timeout: float | None,
        record_class: type[_RecordT] | None
    ) -> None:
        super().__init__(connection)
        self._args = args
        self._prefetch = prefetch
        self._query = query
        self._timeout = timeout
        self._state = state
        self._record_class = record_class
        if state is not None:
            state.attach()

    @connresource.guarded
    def __aiter__(self) -> CursorIterator[_RecordT]:
        prefetch = 50 if self._prefetch is None else self._prefetch
        return CursorIterator(
            self._connection,
            self._query,
            self._state,
            self._args,
            self._record_class,
            prefetch,
            self._timeout,
        )

    @connresource.guarded
    def __await__(
        self
    ) -> compat.Generator[typing.Any, None, Cursor[_RecordT]]:
        if self._prefetch is not None:
            raise exceptions.InterfaceError(
                'prefetch argument can only be specified for iterable cursor')
        cursor: Cursor[_RecordT] = Cursor(
            self._connection,
            self._query,
            self._state,
            self._args,
            self._record_class,
        )
        return cursor._init(self._timeout).__await__()

    def __del__(self) -> None:
        if self._state is not None:
            self._state.detach()
            self._connection._maybe_gc_stmt(self._state)


class BaseCursor(connresource.ConnectionResource, typing.Generic[_RecordT]):

    __slots__ = (
        '_state',
        '_args',
        '_portal_name',
        '_exhausted',
        '_query',
        '_record_class',
    )

    _state: _cprotocol.PreparedStatementState[_RecordT] | None
    _args: compat.Sequence[object]
    _portal_name: str | None
    _exhausted: bool
    _query: str
    _record_class: type[_RecordT] | None

    @typing.overload
    def __init__(
        self: BaseCursor[_RecordT],
        connection: _connection.Connection[_RecordT],
        query: str,
        state: _cprotocol.PreparedStatementState[_RecordT] | None,
        args: compat.Sequence[object],
        record_class: None,
    ) -> None:
        ...

    @typing.overload
    def __init__(
        self: BaseCursor[_RecordT],
        connection: _connection.Connection[typing.Any],
        query: str,
        state: _cprotocol.PreparedStatementState[_RecordT] | None,
        args: compat.Sequence[object],
        record_class: type[_RecordT],
    ) -> None:
        ...

    def __init__(
        self,
        connection: _connection.Connection[typing.Any],
        query: str,
        state: _cprotocol.PreparedStatementState[_RecordT] | None,
        args: compat.Sequence[object],
        record_class: type[_RecordT] | None,
    ) -> None:
        super().__init__(connection)
        self._args = args
        self._state = state
        if state is not None:
            state.attach()
        self._portal_name = None
        self._exhausted = False
        self._query = query
        self._record_class = record_class

    def _check_ready(self) -> None:
        if self._state is None:
            raise exceptions.InterfaceError(
                'cursor: no associated prepared statement')

        if self._state.closed:
            raise exceptions.InterfaceError(
                'cursor: the prepared statement is closed')

        if not self._connection._top_xact:
            raise exceptions.NoActiveSQLTransactionError(
                'cursor cannot be created outside of a transaction')

    async def _bind_exec(self, n: int, timeout: float | None) -> typing.Any:
        self._check_ready()

        if self._portal_name:
            raise exceptions.InterfaceError(
                'cursor already has an open portal')

        con = self._connection
        protocol = con._protocol

        self._portal_name = con._get_unique_id('portal')

        if typing.TYPE_CHECKING:
            assert self._state is not None

        buffer, _, self._exhausted = await protocol.bind_execute(
            self._state, self._args, self._portal_name, n, True, timeout)
        return buffer

    async def _bind(self, timeout: float | None) -> typing.Any:
        self._check_ready()

        if self._portal_name:
            raise exceptions.InterfaceError(
                'cursor already has an open portal')

        con = self._connection
        protocol = con._protocol

        self._portal_name = con._get_unique_id('portal')

        if typing.TYPE_CHECKING:
            assert self._state is not None

        buffer = await protocol.bind(self._state, self._args,
                                     self._portal_name,
                                     timeout)
        return buffer

    async def _exec(self, n: int, timeout: float | None) -> typing.Any:
        self._check_ready()

        if not self._portal_name:
            raise exceptions.InterfaceError(
                'cursor does not have an open portal')

        protocol = self._connection._protocol
        buffer, _, self._exhausted = await protocol.execute(
            self._state, self._portal_name, n, True, timeout)
        return buffer

    async def _close_portal(self, timeout: float | None) -> None:
        self._check_ready()

        if not self._portal_name:
            raise exceptions.InterfaceError(
                'cursor does not have an open portal')

        protocol = self._connection._protocol
        await protocol.close_portal(self._portal_name, timeout)
        self._portal_name = None

    def __repr__(self) -> str:
        attrs: list[str] = []
        if self._exhausted:
            attrs.append('exhausted')
        attrs.append('')  # to separate from id

        if self.__class__.__module__.startswith('asyncpg.'):
            mod = 'asyncpg'
        else:
            mod = self.__class__.__module__

        return '<{}.{} "{!s:.30}" {}{:#x}>'.format(
            mod, self.__class__.__name__,
            self._state.query if self._state is not None else '',
            ' '.join(attrs), id(self))

    def __del__(self) -> None:
        if self._state is not None:
            self._state.detach()
            self._connection._maybe_gc_stmt(self._state)


class CursorIterator(BaseCursor[_RecordT]):

    __slots__ = ('_buffer', '_prefetch', '_timeout')

    _buffer: compat.deque[_RecordT]
    _prefetch: int
    _timeout: float | None

    @typing.overload
    def __init__(
        self: CursorIterator[_RecordT],
        connection: _connection.Connection[_RecordT],
        query: str,
        state: _cprotocol.PreparedStatementState[_RecordT] | None,
        args: compat.Sequence[object],
        record_class: None,
        prefetch: int,
        timeout: float | None,
    ) -> None:
        ...

    @typing.overload
    def __init__(
        self: CursorIterator[_RecordT],
        connection: _connection.Connection[typing.Any],
        query: str,
        state: _cprotocol.PreparedStatementState[_RecordT] | None,
        args: compat.Sequence[object],
        record_class: type[_RecordT],
        prefetch: int,
        timeout: float | None,
    ) -> None:
        ...

    def __init__(
        self,
        connection: _connection.Connection[typing.Any],
        query: str,
        state: _cprotocol.PreparedStatementState[_RecordT] | None,
        args: compat.Sequence[object],
        record_class: type[_RecordT] | None,
        prefetch: int,
        timeout: float | None,
    ) -> None:
        super().__init__(connection, query, state, args, record_class)

        if prefetch <= 0:
            raise exceptions.InterfaceError(
                'prefetch argument must be greater than zero')

        self._buffer = collections.deque()
        self._prefetch = prefetch
        self._timeout = timeout

    @connresource.guarded
    def __aiter__(self) -> Self:
        return self

    @connresource.guarded
    async def __anext__(self) -> _RecordT:
        if self._state is None:
            self._state = await self._connection._get_statement(
                self._query,
                self._timeout,
                named=True,
                record_class=self._record_class,
            )
            self._state.attach()

        if not self._portal_name and not self._exhausted:
            buffer = await self._bind_exec(self._prefetch, self._timeout)
            self._buffer.extend(buffer)

        if not self._buffer and not self._exhausted:
            buffer = await self._exec(self._prefetch, self._timeout)
            self._buffer.extend(buffer)

        if self._portal_name and self._exhausted:
            await self._close_portal(self._timeout)

        if self._buffer:
            return self._buffer.popleft()

        raise StopAsyncIteration


class Cursor(BaseCursor[_RecordT]):
    """An open *portal* into the results of a query."""

    __slots__ = ()

    async def _init(self, timeout: float | None) -> Self:
        if self._state is None:
            self._state = await self._connection._get_statement(
                self._query,
                timeout,
                named=True,
                record_class=self._record_class,
            )
            self._state.attach()
        self._check_ready()
        await self._bind(timeout)
        return self

    @connresource.guarded
    async def fetch(
        self, n: int, *, timeout: float | None = None
    ) -> list[_RecordT]:
        r"""Return the next *n* rows as a list of :class:`Record` objects.

        :param float timeout: Optional timeout value in seconds.

        :return: A list of :class:`Record` instances.
        """
        self._check_ready()
        if n <= 0:
            raise exceptions.InterfaceError('n must be greater than zero')
        if self._exhausted:
            return []
        recs: list[_RecordT] = await self._exec(n, timeout)
        if len(recs) < n:
            self._exhausted = True
        return recs

    @connresource.guarded
    async def fetchrow(
        self, *, timeout: float | None = None
    ) -> _RecordT | None:
        r"""Return the next row.

        :param float timeout: Optional timeout value in seconds.

        :return: A :class:`Record` instance.
        """
        self._check_ready()
        if self._exhausted:
            return None
        recs: list[_RecordT] = await self._exec(1, timeout)
        if len(recs) < 1:
            self._exhausted = True
            return None
        return recs[0]

    @connresource.guarded
    async def forward(self, n: int, *, timeout: float | None = None) -> int:
        r"""Skip over the next *n* rows.

        :param float timeout: Optional timeout value in seconds.

        :return: A number of rows actually skipped over (<= *n*).
        """
        self._check_ready()
        if n <= 0:
            raise exceptions.InterfaceError('n must be greater than zero')

        protocol = self._connection._protocol
        status = await protocol.query('MOVE FORWARD {:d} {}'.format(
            n, self._portal_name), timeout)

        advanced = int(status.split()[1])
        if advanced < n:
            self._exhausted = True

        return advanced
