
# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import functools

from . import exceptions


def guarded(meth):
    """A decorator to add a sanity check to ConnectionResource methods."""

    @functools.wraps(meth)
    def _check(self, *args, **kwargs):
        self._check_conn_validity(meth.__name__)
        return meth(self, *args, **kwargs)

    return _check


class ConnectionResource:
    __slots__ = ('_connection', '_con_release_ctr')

    def __init__(self, connection):
        self._connection = connection
        self._con_release_ctr = getattr(connection, '_pool_release_ctr', None)

    def _check_conn_validity(self, meth_name):
        con_release_ctr = getattr(self._connection, '_pool_release_ctr', None)
        if con_release_ctr != self._con_release_ctr:
            raise exceptions.InterfaceError(
                'cannot call {}.{}(): '
                'the underlying connection has been released back '
                'to the pool'.format(self.__class__.__name__, meth_name))
