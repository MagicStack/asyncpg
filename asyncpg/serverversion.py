# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from . import types


def split_server_version_string(version_string):
    version_string = version_string.strip()
    if version_string.startswith('PostgreSQL '):
        version_string = version_string[len('PostgreSQL '):]

    parts = version_string.strip().split('.')
    if not parts[-1].isdigit():
        # release level specified
        level = parts[-1].rstrip('0123456789').lower()
        serial = parts[-1][level:]
        versions = [int(p) for p in parts[:-1]][:3]
    else:
        level = 'final'
        serial = 0
        versions = [int(p) for p in parts][:3]

    if len(versions) < 3:
        versions += [0] * (3 - len(versions))

    versions.append(level)
    versions.append(serial)

    return types.ServerVersion(*versions)
