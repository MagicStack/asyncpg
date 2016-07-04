cdef inline record_encode_frame(ConnectionSettings settings, WriteBuffer buf,
                                WriteBuffer elem_data, uint32_t elem_count):
    buf.write_int32(4 + elem_data.len())
    # attribute count
    buf.write_int32(elem_count)
    # encoded attribute data
    buf.write_buffer(elem_data)


cdef anonymous_record_decode(ConnectionSettings settings,
                             const char* data, int32_t len):
    cdef:
        decode_func df

        list result
        uint32_t elem_count
        const char *ptr
        uint32_t i
        int32_t elem_len
        uint32_t elem_typ
        Codec elem_codec

    elem_count = hton.unpack_int32(data)
    result = []
    ptr = &data[4]
    for i in range(elem_count):
        elem_typ = elem_typ = hton.unpack_int32(ptr)

        ptr += 4

        elem_len = hton.unpack_int32(ptr)

        ptr += 4

        if elem_len == -1:
            result.append(None)
        else:
            elem_codec = settings.get_data_codec(elem_typ)
            if elem_codec is None or not elem_codec.has_decoder():
                raise RuntimeError('no decoder for type OID {}'.format(
                    elem_typ))
            result.append(elem_codec.decode(settings, ptr, elem_len))
            ptr += elem_len

    return result


cdef init_record_codecs():
    register_core_codec(RECORDOID,
                        <encode_func>NULL,
                        <decode_func>&anonymous_record_decode,
                        PG_FORMAT_BINARY)
