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
