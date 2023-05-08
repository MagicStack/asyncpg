# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import pathlib
import platform
import typing


SYSTEM = platform.uname().system


if SYSTEM == 'Windows':
    import ctypes.wintypes

    CSIDL_APPDATA = 0x001a

    def get_pg_home_directory() -> typing.Optional[pathlib.Path]:
        # We cannot simply use expanduser() as that returns the user's
        # home directory, whereas Postgres stores its config in
        # %AppData% on Windows.
        buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
        r = ctypes.windll.shell32.SHGetFolderPathW(0, CSIDL_APPDATA, 0, 0, buf)
        if r:
            return None
        else:
            return pathlib.Path(buf.value) / 'postgresql'

else:
    def get_pg_home_directory() -> typing.Optional[pathlib.Path]:
        try:
            return pathlib.Path.home()
        except (RuntimeError, KeyError):
            return None


async def wait_closed(stream):
    # Not all asyncio versions have StreamWriter.wait_closed().
    if hasattr(stream, 'wait_closed'):
        try:
            await stream.wait_closed()
        except ConnectionResetError:
            # On Windows wait_closed() sometimes propagates
            # ConnectionResetError which is totally unnecessary.
            pass


# Workaround for https://bugs.python.org/issue37658
async def wait_for(fut, timeout):
    if timeout is None:
        return await fut

    fut = asyncio.ensure_future(fut)

    try:
        return await asyncio.wait_for(fut, timeout)
    except asyncio.CancelledError:
        if fut.done():
            return fut.result()
        else:
            raise
