cdef init_monetary_codecs():
    moneyoids = [
        MONEYOID,
    ]

    for oid in moneyoids:
        register_core_codec(oid,
                            <encode_func>&text_encode,
                            <decode_func>&text_decode,
                            PG_FORMAT_TEXT)


init_monetary_codecs()
