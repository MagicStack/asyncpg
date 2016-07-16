# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef init_tsearch_codecs():
    ts_oids = [
        TSQUERYOID,
        TSVECTOROID,
    ]

    for oid in ts_oids:
        register_core_codec(oid,
                            <encode_func>&text_encode,
                            <decode_func>&text_decode,
                            PG_FORMAT_TEXT)

    register_core_codec(GTSVECTOROID,
                        NULL,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)


init_tsearch_codecs()
