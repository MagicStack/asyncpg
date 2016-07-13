ctypedef object (*encode_func_ex)(ConnectionSettings settings,
                                  WriteBuffer buf,
                                  object obj,
                                  const void *arg)


ctypedef object (*decode_func_ex)(ConnectionSettings settings,
                                  const char *data,
                                  int32_t len,
                                  const void *arg)


cdef inline array_encode(ConnectionSettings settings, WriteBuffer buf,
                         object elements, uint32_t elem_oid,
                         encode_func_ex encoder, const void *encoder_arg):
    cdef:
        WriteBuffer elem_data
        uint32_t elem_count = len(elements)

    elem_data = WriteBuffer.new()

    for item in elements:
        if item is None:
            elem_data.write_int32(-1)
        else:
            encoder(settings, elem_data, item, encoder_arg)

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
    # element data
    buf.write_buffer(elem_data)


cdef inline array_decode(ConnectionSettings settings, const char *data,
                         int32_t len, decode_func_ex decoder,
                         const void *decoder_arg):
     cdef:
         int32_t ndims = hton.unpack_int32(data)
         int32_t flags = hton.unpack_int32(&data[4])
         uint32_t elem_oid = hton.unpack_int32(&data[8])
         uint32_t elem_count = hton.unpack_int32(&data[12])
         tuple result
         const char *ptr = &data[20]
         uint32_t i
         int32_t elem_len

     if ndims > 0:
         result = cpython.PyTuple_New(elem_count)

         for i in range(elem_count):
             elem_len = hton.unpack_int32(ptr)
             ptr += 4
             if elem_len == -1:
                 elem = None
             else:
                 elem = decoder(settings, ptr, elem_len, decoder_arg)
                 ptr += elem_len

             cpython.Py_INCREF(elem)
             cpython.PyTuple_SET_ITEM(result, i, elem)
     else:
         result = ()

     return result


cdef int4_encode_ex(ConnectionSettings settings, WriteBuffer buf, object obj,
                    const void *arg):
    return int4_encode(settings, buf, obj)


cdef int4_decode_ex(ConnectionSettings settings, const char* data,
                    int32_t len, const void *arg):
    return int4_decode(settings, data, len)


cdef arrayoid_encode(ConnectionSettings settings, WriteBuffer buf, items):
    array_encode(settings, buf, items, OIDOID,
                 <encode_func_ex>&int4_encode_ex, NULL)


cdef arrayoid_decode(ConnectionSettings settings, const char* data,
                     int32_t len):
    return array_decode(settings, data, len, <decode_func_ex>&int4_decode_ex,
                        NULL)


cdef text_encode_ex(ConnectionSettings settings, WriteBuffer buf, object obj,
                    const void *arg):
    return text_encode(settings, buf, obj)


cdef text_decode_ex(ConnectionSettings settings, const char* data,
                    int32_t len, const void *arg):
    return text_decode(settings, data, len)


cdef arraytext_encode(ConnectionSettings settings, WriteBuffer buf, items):
    array_encode(settings, buf, items, TEXTOID,
                 <encode_func_ex>&text_encode_ex,NULL)


cdef arraytext_decode(ConnectionSettings settings, const char* data,
                      int32_t len):
    return array_decode(settings, data, len, <decode_func_ex>&text_decode_ex,
                        NULL)


cdef init_array_codecs():
    register_core_codec(_OIDOID,
                        <encode_func>&arrayoid_encode,
                        <decode_func>&arrayoid_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(_TEXTOID,
                        <encode_func>&arraytext_encode,
                        <decode_func>&arraytext_decode,
                        PG_FORMAT_BINARY)
