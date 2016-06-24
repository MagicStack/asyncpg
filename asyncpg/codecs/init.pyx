cdef init_codecs():
    init_int_codecs()
    init_float_codecs()
    init_numeric_codecs()
    init_datetime_codecs()
    init_text_codecs()
    init_bytea_codecs()
    init_uuid_codecs()
    init_array_codecs()


init_codecs()
