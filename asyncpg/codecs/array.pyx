cdef inline array_encode_frame(ConnectionSettings settings, WriteBuffer buf,
                               uint32_t elem_oid, WriteBuffer elem_data,
                               uint32_t elem_count):
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


cdef inline array_encode(ConnectionSettings settings, WriteBuffer buf,
                         uint32_t elem_oid, encode_func encoder,
                         elements):
    cdef:
        WriteBuffer elem_data
        uint32_t elem_count = len(elements)

    elem_data = WriteBuffer.new()

    for item in elements:
        if item is None:
            elem_data.write_int32(-1)
        else:
            encoder(settings, elem_data, item)

    array_encode_frame(settings, buf, elem_oid, elem_data, elem_count)


cdef inline array_decode(ConnectionSettings settings, const char *data,
                         int32_t len, decode_func decoder):
     cdef:
         list result = []
         int32_t ndims = hton.unpack_int32(data)
         int32_t flags = hton.unpack_int32(&data[4])
         uint32_t elem_oid = hton.unpack_int32(&data[8])
         uint32_t elem_count = hton.unpack_int32(&data[12])
         const char *ptr = &data[20]
         uint32_t i
         int32_t elem_len

     if ndims > 0:
         for i in range(elem_count):
             elem_len = hton.unpack_int32(ptr)
             if elem_len == -1:
                 result.append(None)
             else:
                 result.append(decoder(settings, &ptr[4], elem_len))
             ptr += 4 + elem_len

     return result


cdef arrayoid_encode(ConnectionSettings settings, WriteBuffer buf, items):
    array_encode(settings, buf, OIDOID, int4_encode, items)


cdef arrayoid_decode(ConnectionSettings settings, const char* data,
                     int32_t len):
    return array_decode(settings, data, len, int4_decode)


cdef arraytext_encode(ConnectionSettings settings, WriteBuffer buf, items):
    array_encode(settings, buf, TEXTOID, text_encode, items)


cdef arraytext_decode(ConnectionSettings settings, const char* data,
                      int32_t len):
    return array_decode(settings, data, len, text_decode)


cdef init_array_codecs():
    register_core_codec(_OIDOID,
                        <encode_func>&arrayoid_encode,
                        <decode_func>&arrayoid_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(_TEXTOID,
                        <encode_func>&arraytext_encode,
                        <decode_func>&arraytext_decode,
                        PG_FORMAT_BINARY)
