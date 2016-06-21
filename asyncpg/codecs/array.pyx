cdef inline array_encode(ConnectionSettings settings, WriteBuffer buf,
                         uint32_t elem_oid, WriteBuffer elem_data,
                         size_t elem_count):
    buf.write_int32(20 + elem_data.len())
    # Number of dimensions
    buf.write_int32(1)
    # flags
    buf.write_int32(0)
    # element type
    buf.write_int32(elem_oid)
    # upper / lower bounds
    buf.write_int32(elem_count)
    buf.write_int32(1)
    buf.write_buffer(elem_data)


cdef arrayoid_encode(ConnectionSettings settings, WriteBuffer buf, items):
    cdef int32_t oid_val

    elem_data = WriteBuffer.new()

    for item in items:
        int4_encode(settings, elem_data, item)

    array_encode(settings, buf, OIDOID, elem_data, len(items))


cdef arrayoid_decode(ConnectionSettings settings, const char* data,
                     int32_t len):
    result = []

    cdef:
        int32_t ndims = hton.unpack_int32(data)
        int32_t flags = hton.unpack_int32(&data[4])
        uint32_t elem_oid = hton.unpack_int32(&data[8])
        uint32_t elem_count = hton.unpack_int32(&data[12])
        const char *ptr = &data[24]
        uint32_t i

    if ndims > 0:
        for i in range(elem_count):
            result.append(int4_decode(settings, ptr, 4))
            ptr += 8

    return result


cdef inline void init_array_codecs():
    codec_map[_OIDOID].encode = arrayoid_encode
    codec_map[_OIDOID].decode = arrayoid_decode
    codec_map[_OIDOID].format = PG_FORMAT_BINARY
