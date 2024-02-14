# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import re
import typing

if typing.TYPE_CHECKING:
    from . import connection


def _quote_ident(ident: str) -> str:
    return '"{}"'.format(ident.replace('"', '""'))


def _quote_literal(string: str) -> str:
    return "'{}'".format(string.replace("'", "''"))


async def _mogrify(
    conn: connection.Connection[typing.Any],
    query: str,
    args: tuple[typing.Any, ...]
) -> str:
    """Safely inline arguments to query text."""
    # Introspect the target query for argument types and
    # build a list of safely-quoted fully-qualified type names.
    ps = await conn.prepare(query)
    paramtypes: list[str] = []
    for t in ps.get_parameters():
        if t.name.endswith('[]'):
            pname = '_' + t.name[:-2]
        else:
            pname = t.name

        paramtypes.append('{}.{}'.format(
            _quote_ident(t.schema), _quote_ident(pname)))
    del ps

    # Use Postgres to convert arguments to text representation
    # by casting each value to text.
    cols = ['quote_literal(${}::{}::text)'.format(i, t)
            for i, t in enumerate(paramtypes, start=1)]

    textified = await conn.fetchrow(
        'SELECT {cols}'.format(cols=', '.join(cols)), *args)

    if typing.TYPE_CHECKING:
        assert textified is not None

    # Finally, replace $n references with text values.
    return re.sub(
        r'\$(\d+)\b', lambda m: textified[int(m.group(1)) - 1], query)
