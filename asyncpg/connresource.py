
# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import functools
import typing

from . import exceptions

if typing.TYPE_CHECKING:
    from . import compat
    from . import connection as _conn

_F = typing.TypeVar('_F', bound='compat.Callable[..., typing.Any]')


def guarded(meth: _F) -> _F:
    """A decorator to add a sanity check to ConnectionResource methods."""

    @functools.wraps(meth)
    def _check(
        self: ConnectionResource,
        *args: typing.Any,
        **kwargs: typing.Any
    ) -> typing.Any:
        self._check_conn_validity(meth.__name__)
        return meth(self, *args, **kwargs)

    return typing.cast(_F, _check)


class ConnectionResource:
    __slots__ = ('_connection', '_con_release_ctr')

    _connection: _conn.Connection[typing.Any]
    _con_release_ctr: int

    def __init__(self, connection: _conn.Connection[typing.Any]) -> None:
        self._connection = connection
        self._con_release_ctr = connection._pool_release_ctr

    def _check_conn_validity(self, meth_name: str) -> None:
        con_release_ctr = self._connection._pool_release_ctr
        if con_release_ctr != self._con_release_ctr:
            raise exceptions.InterfaceError(
                'cannot call {}.{}(): '
                'the underlying connection has been released back '
                'to the pool'.format(self.__class__.__name__, meth_name))

        if self._connection.is_closed():
            raise exceptions.InterfaceError(
                'cannot call {}.{}(): '
                'the underlying connection is closed'.format(
                    self.__class__.__name__, meth_name))
