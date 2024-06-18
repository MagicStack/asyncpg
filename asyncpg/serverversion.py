# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import re
import typing

from .types import ServerVersion

version_regex: typing.Final = re.compile(
    r"(Postgre[^\s]*)?\s*"
    r"(?P<major>[0-9]+)\.?"
    r"((?P<minor>[0-9]+)\.?)?"
    r"(?P<micro>[0-9]+)?"
    r"(?P<releaselevel>[a-z]+)?"
    r"(?P<serial>[0-9]+)?"
)


class _VersionDict(typing.TypedDict):
    major: int
    minor: int | None
    micro: int | None
    releaselevel: str | None
    serial: int | None


def split_server_version_string(version_string: str) -> ServerVersion:
    version_match = version_regex.search(version_string)

    if version_match is None:
        raise ValueError(
            "Unable to parse Postgres "
            f'version from "{version_string}"'
        )

    version: _VersionDict = version_match.groupdict()  # type: ignore[assignment]  # noqa: E501
    for ver_key, ver_value in version.items():
        # Cast all possible versions parts to int
        try:
            version[ver_key] = int(ver_value)  # type: ignore[literal-required, call-overload]  # noqa: E501
        except (TypeError, ValueError):
            pass

    if version["major"] < 10:
        return ServerVersion(
            version["major"],
            version.get("minor") or 0,
            version.get("micro") or 0,
            version.get("releaselevel") or "final",
            version.get("serial") or 0,
        )

    # Since PostgreSQL 10 the versioning scheme has changed.
    # 10.x really means 10.0.x.  While parsing 10.1
    # as (10, 1) may seem less confusing, in practice most
    # version checks are written as version[:2], and we
    # want to keep that behaviour consistent, i.e not fail
    # a major version check due to a bugfix release.
    return ServerVersion(
        version["major"],
        0,
        version.get("minor") or 0,
        version.get("releaselevel") or "final",
        version.get("serial") or 0,
    )
