cdef core_codec codec_map[MAXBUILTINOID]


cdef inline core_codec* get_core_codec(uint32_t oid):
    cdef core_codec *codec = &codec_map[oid]

    if codec.encode == NULL:
        return NULL
    else:
        return codec


cdef void init_codecs():
    init_int_codecs()
    init_float_codecs()
    init_numeric_codecs()
    init_datetime_codecs()
    init_text_codecs()
    init_bytea_codecs()
    init_array_codecs()


cdef class CodecInfo:
    pass


init_codecs()
