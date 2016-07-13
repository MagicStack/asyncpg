DEF ARRAY_MAXDIM = 6  # defined in postgresql/src/includes/c.h


ctypedef object (*encode_func_ex)(ConnectionSettings settings,
                                  WriteBuffer buf,
                                  object obj,
                                  const void *arg)


ctypedef object (*decode_func_ex)(ConnectionSettings settings,
                                  const char *data,
                                  int32_t len,
                                  const void *arg)


cdef inline bint _is_array(object obj):
    return cpython.PyTuple_Check(obj) or cpython.PyList_Check(obj)


cdef _get_array_shape(object obj, int32_t *dims, int32_t *ndims):
    cdef:
        int32_t mylen = len(obj)
        int32_t elemlen = -2
        object it

    if ndims[0] > ARRAY_MAXDIM:
        raise ValueError(
            'number of array dimensions ({}) exceed the maximum expected ({})'.
                format(ndims[0], ARRAY_MAXDIM))

    dims[ndims[0] - 1] = mylen

    for elem in obj:
        if _is_array(elem):
            if elemlen == -2:
                elemlen = len(elem)
                ndims[0] += 1
                _get_array_shape(elem, dims, ndims)
            else:
                if len(elem) != elemlen:
                    raise ValueError('non-homogeneous array')
        else:
            if elemlen >= 0:
                raise ValueError('non-homogeneous array')
            else:
                elemlen = -1


cdef _write_array_data(ConnectionSettings settings, object obj, int32_t ndims,
                       int32_t dim, WriteBuffer elem_data,
                       encode_func_ex encoder, const void *encoder_arg):
    if dim < ndims - 1:
        for item in obj:
            _write_array_data(settings, item, ndims, dim + 1, elem_data,
                              encoder, encoder_arg)
    else:
        for item in obj:
            if item is None:
                elem_data.write_int32(-1)
            else:
                try:
                    encoder(settings, elem_data, item, encoder_arg)
                except TypeError as e:
                    raise ValueError(
                        'invalid array element: {}'.format(e.args[0])) from None


cdef inline array_encode(ConnectionSettings settings, WriteBuffer buf,
                         object obj, uint32_t elem_oid,
                         encode_func_ex encoder, const void *encoder_arg):
    cdef:
        WriteBuffer elem_data
        int32_t dims[ARRAY_MAXDIM]
        int32_t ndims = 1
        int32_t i

    if not _is_array(obj):
        raise TypeError(
            'list or tuple expected (got type {})'.format(type(obj)))

    _get_array_shape(obj, dims, &ndims)

    elem_data = WriteBuffer.new()

    if ndims > 1:
        _write_array_data(settings, obj, ndims, 0, elem_data,
                          encoder, encoder_arg)
    else:
        for i, item in enumerate(obj):
            if item is None:
                elem_data.write_int32(-1)
            else:
                try:
                    encoder(settings, elem_data, item, encoder_arg)
                except TypeError as e:
                    raise ValueError(
                        'invalid array element at index {}: {}'.format(
                            i, e.args[0])) from None

    buf.write_int32(12 + 8 * ndims + elem_data.len())
    # Number of dimensions
    buf.write_int32(ndims)
    # flags
    buf.write_int32(0)
    # element type
    buf.write_int32(elem_oid)
    # upper / lower bounds
    for i in range(ndims):
        buf.write_int32(dims[i])
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
        const char *ptr = &data[12]
        tuple result
        uint32_t i
        int32_t elem_len
        int64_t elem_count = 1
        int32_t dims[ARRAY_MAXDIM]

    if ndims == 0:
        result = ()
        return result

    if ndims > ARRAY_MAXDIM:
        raise RuntimeError(
            'number of array dimensions exceed the maximum expected ({})'.
            format(ARRAY_MAXDIM))

    for i in range(ndims):
        dims[i] = hton.unpack_int32(ptr)
        elem_count *= dims[i]
        # Ignore the lower bound information
        ptr += 8

    if ndims == 1:
        # Fast path for flat arrays
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
        result = _nested_array_decode(settings, ptr, len - (ptr - data),
                                      decoder, decoder_arg, ndims, dims)

    return result


cdef inline _nested_array_decode(ConnectionSettings settings, const char *data,
                                 int32_t len, decode_func_ex decoder,
                                 const void *decoder_arg,
                                 int32_t ndims, int32_t *dims):

    cdef:
        int32_t elem_len
        int32_t d1, d2, d3, d4, d5, d6
        const char *ptr = data
        tuple result
        object elem
        tuple stride1, stride2, stride3, stride4, stride5

    # Nested array.  The approach here is dumb, but fast: rely
    # on the dimension limit and shape data using nested loops.
    # Alas, Cython doesn't have preprocessor macros.
    #
    result = cpython.PyTuple_New(dims[0])

    for d1 in range(dims[0]):
        stride1 = cpython.PyTuple_New(dims[1])
        cpython.Py_INCREF(stride1)
        cpython.PyTuple_SET_ITEM(result, d1, stride1)

        for d2 in range(dims[1]):
            if ndims == 2:
                elem_len = hton.unpack_int32(ptr)
                ptr += 4
                if elem_len == -1:
                    elem = None
                else:
                    elem = decoder(settings, ptr, elem_len, decoder_arg)
                    ptr += elem_len

                cpython.Py_INCREF(elem)
                cpython.PyTuple_SET_ITEM(stride1, d2, elem)

            else:
                stride2 = cpython.PyTuple_New(dims[2])
                cpython.Py_INCREF(stride2)
                cpython.PyTuple_SET_ITEM(stride1, d2, stride2)

                for d3 in range(dims[2]):
                    if ndims == 3:
                        elem_len = hton.unpack_int32(ptr)
                        ptr += 4
                        if elem_len == -1:
                            elem = None
                        else:
                            elem = decoder(settings, ptr, elem_len, decoder_arg)
                            ptr += elem_len

                        cpython.Py_INCREF(elem)
                        cpython.PyTuple_SET_ITEM(stride2, d3, elem)

                    else:
                        stride3 = cpython.PyTuple_New(dims[3])
                        cpython.Py_INCREF(stride3)
                        cpython.PyTuple_SET_ITEM(stride2, d3, stride3)

                        for d4 in range(dims[3]):
                            if ndims == 4:
                                elem_len = hton.unpack_int32(ptr)
                                ptr += 4
                                if elem_len == -1:
                                    elem = None
                                else:
                                    elem = decoder(settings, ptr, elem_len, decoder_arg)
                                    ptr += elem_len

                                cpython.Py_INCREF(elem)
                                cpython.PyTuple_SET_ITEM(stride3, d4, elem)

                            else:
                                stride4 = cpython.PyTuple_New(dims[4])
                                cpython.Py_INCREF(stride4)
                                cpython.PyTuple_SET_ITEM(stride3, d4, stride4)

                                for d5 in range(dims[4]):
                                    if ndims == 5:
                                        elem_len = hton.unpack_int32(ptr)
                                        ptr += 4
                                        if elem_len == -1:
                                            elem = None
                                        else:
                                            elem = decoder(settings, ptr, elem_len, decoder_arg)
                                            ptr += elem_len

                                        cpython.Py_INCREF(elem)
                                        cpython.PyTuple_SET_ITEM(stride4, d5, elem)

                                    else:
                                        stride5 = cpython.PyTuple_New(dims[5])
                                        cpython.Py_INCREF(stride5)
                                        cpython.PyTuple_SET_ITEM(stride4, d5, stride5)

                                        for d6 in range(dims[5]):
                                            elem_len = hton.unpack_int32(ptr)
                                            ptr += 4
                                            if elem_len == -1:
                                                elem = None
                                            else:
                                                elem = decoder(settings, ptr, elem_len, decoder_arg)
                                                ptr += elem_len

                                            cpython.Py_INCREF(elem)
                                            cpython.PyTuple_SET_ITEM(stride5, d6, elem)

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
