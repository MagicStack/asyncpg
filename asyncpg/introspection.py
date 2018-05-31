# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


_TYPEINFO = '''\
    (
        SELECT
            t.oid                           AS oid,
            ns.nspname                      AS ns,
            t.typname                       AS name,
            t.typtype                       AS kind,
            (CASE WHEN t.typtype = 'd' THEN
                (WITH RECURSIVE typebases(oid, depth) AS (
                    SELECT
                        t2.typbasetype      AS oid,
                        0                   AS depth
                    FROM
                        pg_type t2
                    WHERE
                        t2.oid = t.oid

                    UNION ALL

                    SELECT
                        t2.typbasetype      AS oid,
                        tb.depth + 1        AS depth
                    FROM
                        pg_type t2,
                        typebases tb
                    WHERE
                       tb.oid = t2.oid
                       AND t2.typbasetype != 0
               ) SELECT oid FROM typebases ORDER BY depth DESC LIMIT 1)

               ELSE NULL
            END)                            AS basetype,
            t.typreceive::oid != 0 AND t.typsend::oid != 0
                                            AS has_bin_io,
            t.typelem                       AS elemtype,
            elem_t.typdelim                 AS elemdelim,
            range_t.rngsubtype              AS range_subtype,
            (CASE WHEN t.typtype = 'r' THEN
                (SELECT
                    range_elem_t.typreceive::oid != 0 AND
                        range_elem_t.typsend::oid != 0
                FROM
                    pg_catalog.pg_type AS range_elem_t
                WHERE
                    range_elem_t.oid = range_t.rngsubtype)
            ELSE
                elem_t.typreceive::oid != 0 AND
                    elem_t.typsend::oid != 0
            END)                            AS elem_has_bin_io,
            (CASE WHEN t.typtype = 'c' THEN
                (SELECT
                    array_agg(ia.atttypid ORDER BY ia.attnum)
                FROM
                    pg_attribute ia
                    INNER JOIN pg_class c
                        ON (ia.attrelid = c.oid)
                WHERE
                    ia.attnum > 0 AND NOT ia.attisdropped
                    AND c.reltype = t.oid)

                ELSE NULL
            END)                            AS attrtypoids,
            (CASE WHEN t.typtype = 'c' THEN
                (SELECT
                    array_agg(ia.attname::text ORDER BY ia.attnum)
                FROM
                    pg_attribute ia
                    INNER JOIN pg_class c
                        ON (ia.attrelid = c.oid)
                WHERE
                    ia.attnum > 0 AND NOT ia.attisdropped
                    AND c.reltype = t.oid)

                ELSE NULL
            END)                            AS attrnames
        FROM
            pg_catalog.pg_type AS t
            INNER JOIN pg_catalog.pg_namespace ns ON (
                ns.oid = t.typnamespace)
            LEFT JOIN pg_type elem_t ON (
                t.typlen = -1 AND
                t.typelem != 0 AND
                t.typelem = elem_t.oid
            )
            LEFT JOIN pg_range range_t ON (
                t.oid = range_t.rngtypid
            )
    )
'''


INTRO_LOOKUP_TYPES = '''\
WITH RECURSIVE typeinfo_tree(
    oid, ns, name, kind, basetype, has_bin_io, elemtype, elemdelim,
    range_subtype, elem_has_bin_io, attrtypoids, attrnames, depth)
AS (
    SELECT
        ti.oid, ti.ns, ti.name, ti.kind, ti.basetype, ti.has_bin_io,
        ti.elemtype, ti.elemdelim, ti.range_subtype, ti.elem_has_bin_io,
        ti.attrtypoids, ti.attrnames, 0
    FROM
        {typeinfo} AS ti
    WHERE
        ti.oid = any($1::oid[])

    UNION ALL

    SELECT
        ti.oid, ti.ns, ti.name, ti.kind, ti.basetype, ti.has_bin_io,
        ti.elemtype, ti.elemdelim, ti.range_subtype, ti.elem_has_bin_io,
        ti.attrtypoids, ti.attrnames, tt.depth + 1
    FROM
        {typeinfo} ti,
        typeinfo_tree tt
    WHERE
        (tt.elemtype IS NOT NULL AND ti.oid = tt.elemtype)
        OR (tt.attrtypoids IS NOT NULL AND ti.oid = any(tt.attrtypoids))
        OR (tt.range_subtype IS NOT NULL AND ti.oid = tt.range_subtype)
)

SELECT DISTINCT
    *
FROM
    typeinfo_tree
ORDER BY
    depth DESC
'''.format(typeinfo=_TYPEINFO)


TYPE_BY_NAME = '''\
SELECT
    t.oid,
    t.typelem     AS elemtype,
    t.typtype     AS kind
FROM
    pg_catalog.pg_type AS t
    INNER JOIN pg_catalog.pg_namespace ns ON (ns.oid = t.typnamespace)
WHERE
    t.typname = $1 AND ns.nspname = $2
'''


# 'b' for a base type, 'd' for a domain, 'e' for enum.
SCALAR_TYPE_KINDS = (b'b', b'd', b'e')


def is_scalar_type(typeinfo) -> bool:
    return (
        typeinfo['kind'] in SCALAR_TYPE_KINDS and
        not typeinfo['elemtype']
    )
