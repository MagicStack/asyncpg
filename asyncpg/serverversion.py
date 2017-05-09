# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from . import types


def split_server_version_string(version_string):
    version_string = version_string.strip()
    if version_string.startswith('PostgreSQL '):
        version_string = version_string[len('PostgreSQL '):]
    if version_string.startswith('Postgres-XL'):
        version_string = version_string[len('Postgre-XL '):]

    parts = version_string.strip().split('.')
    if not parts[-1].isdigit():
        # release level specified
        lastitem = parts[-1]
        levelpart = lastitem.rstrip('0123456789').lower()
        if levelpart != lastitem:
            serial = int(lastitem[len(levelpart):])
        else:
            serial = 0

        level = levelpart.lstrip('0123456789')
        if level != levelpart:
            parts[-1] = levelpart[:-len(level)]
        else:
            parts[-1] = 0
    else:
        level = 'final'
        serial = 0

    versions = [int(p) for p in parts][:3]
    if len(versions) < 3:
        versions += [0] * (3 - len(versions))

    versions.append(level)
    versions.append(serial)

    return types.ServerVersion(*versions)
