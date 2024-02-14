# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import functools
import inspect
import typing

from . import connection
from . import exceptions

if typing.TYPE_CHECKING:
    from . import pool
    from . import protocol


_RecordT = typing.TypeVar('_RecordT', bound='protocol.Record')


class PoolConnectionProxyMeta(type):

    def __new__(mcls, name, bases, dct, *, wrap=False):
        if wrap:
            for attrname in dir(connection.Connection):
                if attrname.startswith('_') or attrname in dct:
                    continue

                meth = getattr(connection.Connection, attrname)
                if not inspect.isfunction(meth):
                    continue

                wrapper = mcls._wrap_connection_method(attrname)
                wrapper = functools.update_wrapper(wrapper, meth)
                dct[attrname] = wrapper

            if '__doc__' not in dct:
                dct['__doc__'] = connection.Connection.__doc__

        return super().__new__(mcls, name, bases, dct)

    @staticmethod
    def _wrap_connection_method(meth_name):
        def call_con_method(self, *args, **kwargs):
            # This method will be owned by PoolConnectionProxy class.
            if self._con is None:
                raise exceptions.InterfaceError(
                    'cannot call Connection.{}(): '
                    'connection has been released back to the pool'.format(
                        meth_name))

            meth = getattr(self._con.__class__, meth_name)
            return meth(self._con, *args, **kwargs)

        return call_con_method


class PoolConnectionProxy(connection._ConnectionProxy[_RecordT],
                          metaclass=PoolConnectionProxyMeta,
                          wrap=True):

    __slots__ = ('_con', '_holder')

    def __init__(self, holder: pool.PoolConnectionHolder,
                 con: connection.Connection[_RecordT]):
        self._con = con
        self._holder = holder
        con._set_proxy(self)

    def __getattr__(self, attr):
        # Proxy all unresolved attributes to the wrapped Connection object.
        return getattr(self._con, attr)

    def _detach(self) -> connection.Connection[_RecordT]:
        if self._con is None:
            return

        con, self._con = self._con, None
        con._set_proxy(None)
        return con

    def __repr__(self):
        if self._con is None:
            return '<{classname} [released] {id:#x}>'.format(
                classname=self.__class__.__name__, id=id(self))
        else:
            return '<{classname} {con!r} {id:#x}>'.format(
                classname=self.__class__.__name__, con=self._con, id=id(self))
