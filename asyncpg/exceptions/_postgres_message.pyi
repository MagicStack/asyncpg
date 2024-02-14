# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

import typing

_PM = typing.TypeVar('_PM', bound=PostgresMessage)

class PostgresMessageMeta(type): ...

class PostgresMessage(metaclass=PostgresMessageMeta):
    severity: str | None
    severity_en: str | None
    sqlstate: typing.ClassVar[str]
    message: str
    detail: str | None
    hint: str | None
    position: str | None
    internal_position: str | None
    internal_query: str | None
    context: str | None
    schema_name: str | None
    table_name: str | None
    column_name: str | None
    data_type_name: str | None
    constraint_name: str | None
    server_source_filename: str | None
    server_source_line: str | None
    server_source_function: str | None
    @classmethod
    def _make_constructor(
        cls: type[_PM], fields: dict[str, str], query: str | None = ...
    ) -> tuple[type[_PM], str, dict[str, str]]: ...
    def as_dict(self) -> dict[str, str]: ...
