# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef jsonb_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        char *str
        ssize_t size

    as_pg_string_and_size(settings, obj, &str, &size)

    if size > 0x7fffffff - 1:
        raise ValueError('string too long')

    buf.write_int32(<int32_t>size + 1)
    buf.write_byte(1)  # JSONB format version
    buf.write_cstr(str, size)


cdef jsonb_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef uint8_t format = <uint8_t>buf.read(1)[0]

    if format != 1:
        raise ValueError('unexpected JSONB format: {}'.format(format))

    return text_decode(settings, buf)


cdef init_json_codecs():
    register_core_codec(JSONOID,
                        <encode_func>&text_encode,
                        <decode_func>&text_decode,
                        PG_FORMAT_BINARY)
    register_core_codec(JSONBOID,
                        <encode_func>&jsonb_encode,
                        <decode_func>&jsonb_decode,
                        PG_FORMAT_BINARY)

init_json_codecs()
