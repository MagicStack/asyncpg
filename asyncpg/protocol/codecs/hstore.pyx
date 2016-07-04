cdef hstore_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        char *str
        ssize_t size
        int32_t count
        object items
        WriteBuffer item_buf = WriteBuffer.new()

    count = cpython.PyLong_AsLong(len(obj))
    item_buf.write_int32(count)

    if hasattr(obj, 'items'):
        items = obj.items()
    else:
        items = obj

    for k, v in items:
        as_pg_string_and_size(settings, k, &str, &size)
        item_buf.write_int32(<int32_t>size)
        item_buf.write_cstr(str, size)
        if v is None:
            item_buf.write_int32(<int32_t>-1)
        else:
            as_pg_string_and_size(settings, v, &str, &size)
            item_buf.write_int32(<int32_t>size)
            item_buf.write_cstr(str, size)

    buf.write_int32(item_buf.len())
    buf.write_buffer(item_buf)


cdef hstore_decode(ConnectionSettings settings,
                   const char* data, int32_t len):
    cdef:
        dict result
        uint32_t elem_count
        const char *ptr
        uint32_t i
        int32_t elem_len
        str k
        str v

    result = {}

    elem_count = hton.unpack_int32(data)
    if elem_count == 0:
        return result

    ptr = &data[4]
    for i in range(elem_count):
        elem_len = hton.unpack_int32(ptr)
        if elem_len < 0:
            raise ValueError('null value not allowed in hstore key')

        ptr += 4
        k = decode_pg_string(settings, ptr, elem_len)

        ptr += elem_len
        elem_len = hton.unpack_int32(ptr)
        ptr += 4
        if elem_len < 0:
            v = None
        else:
            v = decode_pg_string(settings, ptr, elem_len)
            ptr += elem_len

        result[k] = v

    return result


cdef init_hstore_codecs():
    register_extra_codec('pg_contrib.hstore',
                         <encode_func>&hstore_encode,
                         <decode_func>&hstore_decode,
                         PG_FORMAT_BINARY)

init_hstore_codecs()
