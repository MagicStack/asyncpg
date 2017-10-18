#!/usr/bin/env python3
#
# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import argparse
import asyncio

import asyncpg


# Array types with builtin codecs, necessary for codec
# bootstrap to work
#
_BUILTIN_ARRAYS = ('_text', '_oid')

_INVALIDOID = 0

# postgresql/src/include/access/transam.h: FirstBootstrapObjectId
_MAXBUILTINOID = 10000 - 1


async def runner(args):
    conn = await asyncpg.connect(host=args.pghost, port=args.pgport,
                                 user=args.pguser)

    buf = (
        '# GENERATED FROM pg_catalog.pg_type\n' +
        '# DO NOT MODIFY, use tools/generate_type_map.py to update\n\n' +
        'DEF INVALIDOID = {}\n'.format(_INVALIDOID) +
        'DEF MAXBUILTINOID = {}\n'.format(_MAXBUILTINOID)
    )

    pg_types = await conn.fetch('''
        SELECT
            oid,
            typname
        FROM
            pg_catalog.pg_type
        WHERE
            typtype IN ('b', 'p')
            AND (typelem = 0 OR typname = any($1) OR typlen > 0)
            AND oid <= $2
        ORDER BY
            oid
    ''', _BUILTIN_ARRAYS, _MAXBUILTINOID)

    defs = []
    typemap = {}
    array_types = []

    for pg_type in pg_types:
        typeoid = pg_type['oid']
        typename = pg_type['typname']

        defname = '{}OID'.format(typename.upper())
        defs.append('DEF {name} = {oid}'.format(name=defname, oid=typeoid))

        if typename in _BUILTIN_ARRAYS:
            array_types.append(defname)
            typename = typename[1:] + '[]'

        typemap[defname] = typename

    buf += 'DEF MAXSUPPORTEDOID = {}\n\n'.format(pg_types[-1]['oid'])

    buf += '\n'.join(defs)

    buf += '\n\nARRAY_TYPES = ({},)'.format(', '.join(array_types))

    f_typemap = ('{}: {!r}'.format(dn, n) for dn, n in sorted(typemap.items()))
    buf += '\n\nTYPEMAP = {{\n    {}}}'.format(',\n    '.join(f_typemap))

    print(buf)


def main():
    parser = argparse.ArgumentParser(
        description='generate protocol/pgtypes.pxi from pg_catalog.pg_types')
    parser.add_argument(
        '--pghost', type=str, default='127.0.0.1',
        help='PostgreSQL server host')
    parser.add_argument(
        '--pgport', type=int, default=5432,
        help='PostgreSQL server port')
    parser.add_argument(
        '--pguser', type=str, default='postgres',
        help='PostgreSQL server user')

    args = parser.parse_args()

    loop = asyncio.get_event_loop()

    loop.run_until_complete(runner(args))


if __name__ == '__main__':
    main()
