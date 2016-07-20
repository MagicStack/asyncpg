# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef inline as_pg_string_and_size(
        ConnectionSettings settings, obj, char **cstr, ssize_t *size):

    if not cpython.PyUnicode_Check(obj):
        obj = str(obj)

    if settings.is_encoding_utf8():
        cstr[0] = PyUnicode_AsUTF8AndSize(obj, size)
    else:
        encoded = settings.get_text_codec().encode(obj)
        cpython.PyBytes_AsStringAndSize(encoded, cstr, size)

    if size[0] > 0x7fffffff:
        raise ValueError('string too long')


cdef text_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        char *str
        ssize_t size

    as_pg_string_and_size(settings, obj, &str, &size)

    buf.write_int32(<int32_t>size)
    buf.write_cstr(str, size)


cdef inline decode_pg_string(ConnectionSettings settings, const char* data,
                             int32_t len):

    if settings.is_encoding_utf8():
        # decode UTF-8 in strict mode
        return cpython.PyUnicode_DecodeUTF8(data, len, NULL)
    else:
        bytes = cpython.PyBytes_FromStringAndSize(data, len)
        return settings.get_text_codec().decode(bytes)


cdef text_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef size_t buf_len = buf.len
    return decode_pg_string(settings, buf.read_all(), buf_len)


cdef init_text_codecs():
    textoids = [
        NAMEOID,
        BPCHAROID,
        VARCHAROID,
        TEXTOID,
        XMLOID
    ]

    for oid in textoids:
        register_core_codec(oid,
                            <encode_func>&text_encode,
                            <decode_func>&text_decode,
                            PG_FORMAT_BINARY)


init_text_codecs()
