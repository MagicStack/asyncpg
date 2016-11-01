# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import functools
import os
import sys


PY_36 = sys.version_info >= (3, 6)


if sys.version_info < (3, 5, 2):
    def aiter_compat(func):
        @functools.wraps(func)
        async def wrapper(self):
            return func(self)
        return wrapper
else:
    def aiter_compat(func):
        return func


if PY_36:
    fspath = os.fspath
else:
    def fspath(path):
        fsp = getattr(path, '__fspath__', None)
        if fsp is not None and callable(fsp):
            path = fsp()
            if not isinstance(path, (str, bytes)):
                raise TypeError(
                    'expected {}() to return str or bytes, not {}'.format(
                        fsp.__qualname__, type(path).__name__
                    ))
            return path
        elif isinstance(path, (str, bytes)):
            return path
        else:
            raise TypeError(
                'expected str, bytes or path-like object, not {}'.format(
                    type(path).__name__
                )
            )
