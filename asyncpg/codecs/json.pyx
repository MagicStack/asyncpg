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


cdef jsonb_decode(ConnectionSettings settings, const char* data, int32_t len):
    if data[0] != 1:
        raise ValueError('unexpected JSONB format: {}'.format(int(data[0])))

    return text_decode(settings, &data[1], len - 1)


cdef init_json_codecs():
    register_core_codec(JSONOID,
                        <encode_func>&text_encode,
                        <decode_func>&text_decode,
                        PG_FORMAT_BINARY)
    register_core_codec(JSONBOID,
                        <encode_func>&jsonb_encode,
                        <decode_func>&jsonb_decode,
                        PG_FORMAT_BINARY)
