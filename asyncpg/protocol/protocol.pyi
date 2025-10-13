import asyncio
import asyncio.protocols
import hmac
from codecs import CodecInfo
from collections.abc import Callable, Iterable, Sequence
from hashlib import md5, sha256
from typing import (
    Any,
    ClassVar,
    Final,
    Generic,
    Literal,
    NewType,
    TypeVar,
    final,
    overload,
)
from typing_extensions import TypeAlias

import asyncpg.pgproto.pgproto

from ..connect_utils import _ConnectionParameters
from ..pgproto.pgproto import WriteBuffer
from ..types import Attribute, Type
from .record import Record

_Record = TypeVar('_Record', bound=Record)
_OtherRecord = TypeVar('_OtherRecord', bound=Record)
_PreparedStatementState = TypeVar(
    '_PreparedStatementState', bound=PreparedStatementState[Any]
)

_NoTimeoutType = NewType('_NoTimeoutType', object)
_TimeoutType: TypeAlias = float | None | _NoTimeoutType

BUILTIN_TYPE_NAME_MAP: Final[dict[str, int]]
BUILTIN_TYPE_OID_MAP: Final[dict[int, str]]
NO_TIMEOUT: Final[_NoTimeoutType]

hashlib_md5 = md5

@final
class ConnectionSettings(asyncpg.pgproto.pgproto.CodecContext):
    __pyx_vtable__: Any
    def __init__(self, conn_key: object) -> None: ...
    def add_python_codec(
        self,
        typeoid: int,
        typename: str,
        typeschema: str,
        typeinfos: Iterable[object],
        typekind: str,
        encoder: Callable[[Any], Any],
        decoder: Callable[[Any], Any],
        format: object,
    ) -> Any: ...
    def clear_type_cache(self) -> None: ...
    def get_data_codec(
        self, oid: int, format: object = ..., ignore_custom_codec: bool = ...
    ) -> Any: ...
    def get_text_codec(self) -> CodecInfo: ...
    def register_data_types(self, types: Iterable[object]) -> None: ...
    def remove_python_codec(
        self, typeoid: int, typename: str, typeschema: str
    ) -> None: ...
    def set_builtin_type_codec(
        self,
        typeoid: int,
        typename: str,
        typeschema: str,
        typekind: str,
        alias_to: str,
        format: object = ...,
    ) -> Any: ...
    def __getattr__(self, name: str) -> Any: ...
    def __reduce__(self) -> Any: ...

@final
class PreparedStatementState(Generic[_Record]):
    closed: bool
    prepared: bool
    name: str
    query: str
    refs: int
    record_class: type[_Record]
    ignore_custom_codec: bool
    __pyx_vtable__: Any
    def __init__(
        self,
        name: str,
        query: str,
        protocol: BaseProtocol[Any],
        record_class: type[_Record],
        ignore_custom_codec: bool,
    ) -> None: ...
    def _get_parameters(self) -> tuple[Type, ...]: ...
    def _get_attributes(self) -> tuple[Attribute, ...]: ...
    def _init_types(self) -> set[int]: ...
    def _init_codecs(self) -> None: ...
    def attach(self) -> None: ...
    def detach(self) -> None: ...
    def mark_closed(self) -> None: ...
    def mark_unprepared(self) -> None: ...
    def __reduce__(self) -> Any: ...

class CoreProtocol:
    backend_pid: Any
    backend_secret: Any
    __pyx_vtable__: Any
    def __init__(self, addr: object, con_params: _ConnectionParameters) -> None: ...
    def is_in_transaction(self) -> bool: ...
    def __reduce__(self) -> Any: ...

class BaseProtocol(CoreProtocol, Generic[_Record]):
    queries_count: Any
    is_ssl: bool
    __pyx_vtable__: Any
    def __init__(
        self,
        addr: object,
        connected_fut: object,
        con_params: _ConnectionParameters,
        record_class: type[_Record],
        loop: object,
    ) -> None: ...
    def set_connection(self, connection: object) -> None: ...
    def get_server_pid(self, *args: object, **kwargs: object) -> int: ...
    def get_settings(self, *args: object, **kwargs: object) -> ConnectionSettings: ...
    def get_record_class(self) -> type[_Record]: ...
    def abort(self) -> None: ...
    async def bind(
        self,
        state: PreparedStatementState[_OtherRecord],
        args: Sequence[object],
        portal_name: str,
        timeout: _TimeoutType,
    ) -> Any: ...
    @overload
    async def bind_execute(
        self,
        state: PreparedStatementState[_OtherRecord],
        args: Sequence[object],
        portal_name: str,
        limit: int,
        return_extra: Literal[False],
        timeout: _TimeoutType,
    ) -> list[_OtherRecord]: ...
    @overload
    async def bind_execute(
        self,
        state: PreparedStatementState[_OtherRecord],
        args: Sequence[object],
        portal_name: str,
        limit: int,
        return_extra: Literal[True],
        timeout: _TimeoutType,
    ) -> tuple[list[_OtherRecord], bytes, bool]: ...
    @overload
    async def bind_execute(
        self,
        state: PreparedStatementState[_OtherRecord],
        args: Sequence[object],
        portal_name: str,
        limit: int,
        return_extra: bool,
        timeout: _TimeoutType,
    ) -> list[_OtherRecord] | tuple[list[_OtherRecord], bytes, bool]: ...
    async def bind_execute_many(
        self,
        state: PreparedStatementState[_OtherRecord],
        args: Iterable[Sequence[object]],
        portal_name: str,
        timeout: _TimeoutType,
    ) -> None: ...
    async def close(self, timeout: _TimeoutType) -> None: ...
    def _get_timeout(self, timeout: _TimeoutType) -> float | None: ...
    def _is_cancelling(self) -> bool: ...
    async def _wait_for_cancellation(self) -> None: ...
    async def close_statement(
        self, state: PreparedStatementState[_OtherRecord], timeout: _TimeoutType
    ) -> Any: ...
    async def copy_in(self, *args: object, **kwargs: object) -> str: ...
    async def copy_out(self, *args: object, **kwargs: object) -> str: ...
    async def execute(self, *args: object, **kwargs: object) -> Any: ...
    def is_closed(self, *args: object, **kwargs: object) -> Any: ...
    def is_connected(self, *args: object, **kwargs: object) -> Any: ...
    def data_received(self, data: object) -> None: ...
    def connection_made(self, transport: object) -> None: ...
    def connection_lost(self, exc: Exception | None) -> None: ...
    def pause_writing(self, *args: object, **kwargs: object) -> Any: ...
    @overload
    async def prepare(
        self,
        stmt_name: str,
        query: str,
        timeout: float | None = ...,
        *,
        state: _PreparedStatementState,
        ignore_custom_codec: bool = ...,
        record_class: None,
    ) -> _PreparedStatementState: ...
    @overload
    async def prepare(
        self,
        stmt_name: str,
        query: str,
        timeout: float | None = ...,
        *,
        state: None = ...,
        ignore_custom_codec: bool = ...,
        record_class: type[_OtherRecord],
    ) -> PreparedStatementState[_OtherRecord]: ...
    async def close_portal(self, portal_name: str, timeout: _TimeoutType) -> None: ...
    async def query(self, *args: object, **kwargs: object) -> str: ...
    def resume_writing(self, *args: object, **kwargs: object) -> Any: ...
    def __reduce__(self) -> Any: ...

@final
class Codec:
    __pyx_vtable__: Any
    def __reduce__(self) -> Any: ...

class DataCodecConfig:
    __pyx_vtable__: Any
    def __init__(self) -> None: ...
    def add_python_codec(
        self,
        typeoid: int,
        typename: str,
        typeschema: str,
        typekind: str,
        typeinfos: Iterable[object],
        encoder: Callable[[ConnectionSettings, WriteBuffer, object], object],
        decoder: Callable[..., object],
        format: object,
        xformat: object,
    ) -> Any: ...
    def add_types(self, types: Iterable[object]) -> Any: ...
    def clear_type_cache(self) -> None: ...
    def declare_fallback_codec(self, oid: int, name: str, schema: str) -> Codec: ...
    def remove_python_codec(
        self, typeoid: int, typename: str, typeschema: str
    ) -> Any: ...
    def set_builtin_type_codec(
        self,
        typeoid: int,
        typename: str,
        typeschema: str,
        typekind: str,
        alias_to: str,
        format: object = ...,
    ) -> Any: ...
    def __reduce__(self) -> Any: ...

class Protocol(BaseProtocol[_Record], asyncio.protocols.Protocol): ...

class Timer:
    def __init__(self, budget: float | None) -> None: ...
    def __enter__(self) -> None: ...
    def __exit__(self, et: object, e: object, tb: object) -> None: ...
    def get_remaining_budget(self) -> float: ...
    def has_budget_greater_than(self, amount: float) -> bool: ...

@final
class SCRAMAuthentication:
    AUTHENTICATION_METHODS: ClassVar[list[str]]
    DEFAULT_CLIENT_NONCE_BYTES: ClassVar[int]
    DIGEST = sha256
    REQUIREMENTS_CLIENT_FINAL_MESSAGE: ClassVar[list[str]]
    REQUIREMENTS_CLIENT_PROOF: ClassVar[list[str]]
    SASLPREP_PROHIBITED: ClassVar[tuple[Callable[[str], bool], ...]]
    authentication_method: bytes
    authorization_message: bytes | None
    client_channel_binding: bytes
    client_first_message_bare: bytes | None
    client_nonce: bytes | None
    client_proof: bytes | None
    password_salt: bytes | None
    password_iterations: int
    server_first_message: bytes | None
    server_key: hmac.HMAC | None
    server_nonce: bytes | None
