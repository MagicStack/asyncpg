# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

import contextlib
from collections.abc import (
    AsyncIterable,
    Callable,
    Iterable,
    Iterator,
    Sequence,
)
from typing import Any, TypeVar, overload

from . import connection
from . import cursor
from . import pool
from . import prepared_stmt
from . import protocol
from . import transaction
from . import types
from .protocol import protocol as _cprotocol

_RecordT = TypeVar('_RecordT', bound=protocol.Record)
_OtherRecordT = TypeVar('_OtherRecordT', bound=protocol.Record)

class PoolConnectionProxyMeta(type): ...

class PoolConnectionProxy(
    connection._ConnectionProxy[_RecordT], metaclass=PoolConnectionProxyMeta
):
    __slots__ = ('_con', '_holder')
    _con: connection.Connection[_RecordT]
    _holder: pool.PoolConnectionHolder[_RecordT]
    def __init__(
        self,
        holder: pool.PoolConnectionHolder[_RecordT],
        con: connection.Connection[_RecordT],
    ) -> None: ...
    def _detach(self) -> connection.Connection[_RecordT]: ...

    # The following methods are copied from Connection
    async def add_listener(
        self, channel: str, callback: connection.Listener
    ) -> None: ...
    async def remove_listener(
        self, channel: str, callback: connection.Listener
    ) -> None: ...
    def add_log_listener(self, callback: connection.LogListener) -> None: ...
    def remove_log_listener(self, callback: connection.LogListener) -> None: ...
    def add_termination_listener(
        self, callback: connection.TerminationListener
    ) -> None: ...
    def remove_termination_listener(
        self, callback: connection.TerminationListener
    ) -> None: ...
    def add_query_logger(self, callback: connection.QueryLogger) -> None: ...
    def remove_query_logger(self, callback: connection.QueryLogger) -> None: ...
    def get_server_pid(self) -> int: ...
    def get_server_version(self) -> types.ServerVersion: ...
    def get_settings(self) -> _cprotocol.ConnectionSettings: ...
    def transaction(
        self,
        *,
        isolation: transaction.IsolationLevels | None = ...,
        readonly: bool = ...,
        deferrable: bool = ...,
    ) -> transaction.Transaction: ...
    def is_in_transaction(self) -> bool: ...
    async def execute(
        self, query: str, *args: object, timeout: float | None = ...
    ) -> str: ...
    async def executemany(
        self,
        command: str,
        args: Iterable[Sequence[object]],
        *,
        timeout: float | None = ...,
    ) -> None: ...
    @overload
    def cursor(
        self,
        query: str,
        *args: object,
        prefetch: int | None = ...,
        timeout: float | None = ...,
        record_class: None = ...,
    ) -> cursor.CursorFactory[_RecordT]: ...
    @overload
    def cursor(
        self,
        query: str,
        *args: object,
        prefetch: int | None = ...,
        timeout: float | None = ...,
        record_class: type[_OtherRecordT],
    ) -> cursor.CursorFactory[_OtherRecordT]: ...
    @overload
    def cursor(
        self,
        query: str,
        *args: object,
        prefetch: int | None = ...,
        timeout: float | None = ...,
        record_class: type[_OtherRecordT] | None,
    ) -> cursor.CursorFactory[_RecordT] | cursor.CursorFactory[_OtherRecordT]: ...
    @overload
    async def prepare(
        self,
        query: str,
        *,
        name: str | None = ...,
        timeout: float | None = ...,
        record_class: None = ...,
    ) -> prepared_stmt.PreparedStatement[_RecordT]: ...
    @overload
    async def prepare(
        self,
        query: str,
        *,
        name: str | None = ...,
        timeout: float | None = ...,
        record_class: type[_OtherRecordT],
    ) -> prepared_stmt.PreparedStatement[_OtherRecordT]: ...
    @overload
    async def prepare(
        self,
        query: str,
        *,
        name: str | None = ...,
        timeout: float | None = ...,
        record_class: type[_OtherRecordT] | None,
    ) -> (
        prepared_stmt.PreparedStatement[_RecordT]
        | prepared_stmt.PreparedStatement[_OtherRecordT]
    ): ...
    @overload
    async def fetch(
        self,
        query: str,
        *args: object,
        timeout: float | None = ...,
        record_class: None = ...,
    ) -> list[_RecordT]: ...
    @overload
    async def fetch(
        self,
        query: str,
        *args: object,
        timeout: float | None = ...,
        record_class: type[_OtherRecordT],
    ) -> list[_OtherRecordT]: ...
    @overload
    async def fetch(
        self,
        query: str,
        *args: object,
        timeout: float | None = ...,
        record_class: type[_OtherRecordT] | None,
    ) -> list[_RecordT] | list[_OtherRecordT]: ...
    async def fetchval(
        self,
        query: str,
        *args: object,
        column: int = ...,
        timeout: float | None = ...,
    ) -> Any: ...
    @overload
    async def fetchrow(
        self,
        query: str,
        *args: object,
        timeout: float | None = ...,
        record_class: None = ...,
    ) -> _RecordT | None: ...
    @overload
    async def fetchrow(
        self,
        query: str,
        *args: object,
        timeout: float | None = ...,
        record_class: type[_OtherRecordT],
    ) -> _OtherRecordT | None: ...
    @overload
    async def fetchrow(
        self,
        query: str,
        *args: object,
        timeout: float | None = ...,
        record_class: type[_OtherRecordT] | None,
    ) -> _RecordT | _OtherRecordT | None: ...
    async def copy_from_table(
        self,
        table_name: str,
        *,
        output: connection._OutputType,
        columns: Iterable[str] | None = ...,
        schema_name: str | None = ...,
        timeout: float | None = ...,
        format: connection._CopyFormat | None = ...,
        oids: int | None = ...,
        delimiter: str | None = ...,
        null: str | None = ...,
        header: bool | None = ...,
        quote: str | None = ...,
        escape: str | None = ...,
        force_quote: bool | Iterable[str] | None = ...,
        encoding: str | None = ...,
    ) -> str: ...
    async def copy_from_query(
        self,
        query: str,
        *args: object,
        output: connection._OutputType,
        timeout: float | None = ...,
        format: connection._CopyFormat | None = ...,
        oids: int | None = ...,
        delimiter: str | None = ...,
        null: str | None = ...,
        header: bool | None = ...,
        quote: str | None = ...,
        escape: str | None = ...,
        force_quote: bool | Iterable[str] | None = ...,
        encoding: str | None = ...,
    ) -> str: ...
    async def copy_to_table(
        self,
        table_name: str,
        *,
        source: connection._SourceType,
        columns: Iterable[str] | None = ...,
        schema_name: str | None = ...,
        timeout: float | None = ...,
        format: connection._CopyFormat | None = ...,
        oids: int | None = ...,
        freeze: bool | None = ...,
        delimiter: str | None = ...,
        null: str | None = ...,
        header: bool | None = ...,
        quote: str | None = ...,
        escape: str | None = ...,
        force_quote: bool | Iterable[str] | None = ...,
        force_not_null: bool | Iterable[str] | None = ...,
        force_null: bool | Iterable[str] | None = ...,
        encoding: str | None = ...,
        where: str | None = ...,
    ) -> str: ...
    async def copy_records_to_table(
        self,
        table_name: str,
        *,
        records: Iterable[Sequence[object]] | AsyncIterable[Sequence[object]],
        columns: Iterable[str] | None = ...,
        schema_name: str | None = ...,
        timeout: float | None = ...,
        where: str | None = ...,
    ) -> str: ...
    async def set_type_codec(
        self,
        typename: str,
        *,
        schema: str = ...,
        encoder: Callable[[Any], Any],
        decoder: Callable[[Any], Any],
        format: str = ...,
    ) -> None: ...
    async def reset_type_codec(self, typename: str, *, schema: str = ...) -> None: ...
    async def set_builtin_type_codec(
        self,
        typename: str,
        *,
        schema: str = ...,
        codec_name: str,
        format: str | None = ...,
    ) -> None: ...
    def is_closed(self) -> bool: ...
    async def close(self, *, timeout: float | None = ...) -> None: ...
    def terminate(self) -> None: ...
    async def reset(self, *, timeout: float | None = ...) -> None: ...
    async def reload_schema_state(self) -> None: ...
    @contextlib.contextmanager
    def query_logger(self, callback: connection.QueryLogger) -> Iterator[None]: ...
