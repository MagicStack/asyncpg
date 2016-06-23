cdef text_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        char *str
        ssize_t size

    if settings.is_encoding_utf8():
        str = PyUnicode_AsUTF8AndSize(obj, &size)
    else:
        encoded = settings.get_codec().encode(obj)
        cpython.PyBytes_AsStringAndSize(buf, &str, &size)

    if size > 0x7fffffff:
        raise ValueError('string too long')

    buf.write_int32(<int32_t>size)
    buf.write_cstr(str, size)


cdef text_decode(ConnectionSettings settings, const char* data, int32_t len):
    if settings.is_encoding_utf8():
        # decode UTF-8 in strict mode
        return cpython.PyUnicode_DecodeUTF8(data, len, NULL)
    else:
        bytes = cpython.PyBytes_FromStringAndSize(data, len)
        return settings.get_codec().decode(bytes)


cdef init_text_codecs():
    textoids = [
        NAMEOID,
        BPCHAROID,
        VARCHAROID,
        CSTRINGOID,
        TEXTOID,
        REGTYPEOID,
        REGPROCOID,
        REGPROCEDUREOID,
        REGOPEROID,
        REGOPERATOROID,
        REGCLASSOID,
    ]

    for oid in textoids:
        register_core_codec(oid,
                            <encode_func>&text_encode,
                            <decode_func>&text_decode,
                            PG_FORMAT_BINARY)
