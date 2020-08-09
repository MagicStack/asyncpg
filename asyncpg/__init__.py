# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from .connection import connect, Connection  # NOQA
from .exceptions import *  # NOQA
from .pool import create_pool  # NOQA
from .protocol import Record  # NOQA
from .types import *  # NOQA


__all__ = ('connect', 'create_pool', 'Record', 'Connection') + \
          exceptions.__all__  # NOQA

# The rules of changing __version__:
#
#    In a release revision, __version__ must be set to 'x.y.z',
#    and the release revision tagged with the 'vx.y.z' tag.
#    For example, asyncpg release 0.15.0 should have
#    __version__ set to '0.15.0', and tagged with 'v0.15.0'.
#
#    In between releases, __version__ must be set to
#    'x.y+1.0.dev0', so asyncpg revisions between 0.15.0 and
#    0.16.0 should have __version__ set to '0.16.0.dev0' in
#    the source.
#
#    Source and wheel distributions built from development
#    snapshots will automatically include the git revision
#    in __version__, for example: '0.16.0.dev0+ge06ad03'

__version__ = '0.21.0'
