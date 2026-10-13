
# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar
from typing_extensions import ParamSpec

from . import exceptions

if TYPE_CHECKING:
    from . import connection

_ConnectionResourceT = TypeVar(
    "_ConnectionResourceT", bound="ConnectionResource", contravariant=True
)
_P = ParamSpec("_P")
_R = TypeVar("_R", covariant=True)


class _ConnectionResourceMethod(
    Protocol,
    Generic[_ConnectionResourceT, _R, _P],
):
    # This indicates that the Protocol is a function and not a lambda
    __name__: str

    # Type signature of a method on an instance of _ConnectionResourceT
    def __call__(
        _, self: _ConnectionResourceT, *args: _P.args, **kwds: _P.kwargs
    ) -> _R:
        ...


def guarded(
    meth: _ConnectionResourceMethod[_ConnectionResourceT, _R, _P]
) -> _ConnectionResourceMethod[_ConnectionResourceT, _R, _P]:
    """A decorator to add a sanity check to ConnectionResource methods."""

    @functools.wraps(meth)
    def _check(
        self: _ConnectionResourceT, *args: _P.args, **kwargs: _P.kwargs
    ) -> _R:
        self._check_conn_validity(meth.__name__)
        return meth(self, *args, **kwargs)

    return _check


class ConnectionResource:
    __slots__ = ('_connection', '_con_release_ctr')

    def __init__(self, connection: connection.Connection) -> None:
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
