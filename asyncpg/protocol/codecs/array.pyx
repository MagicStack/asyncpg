# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from collections.abc import Container as ContainerABC


DEF ARRAY_MAXDIM = 6  # defined in postgresql/src/includes/c.h


ctypedef object (*encode_func_ex)(ConnectionSettings settings,
                                  WriteBuffer buf,
                                  object obj,
                                  const void *arg)


ctypedef object (*decode_func_ex)(ConnectionSettings settings,
                                  FastReadBuffer buf,
                                  const void *arg)


cdef inline bint _is_trivial_container(object obj):
    return cpython.PyUnicode_Check(obj) or cpython.PyBytes_Check(obj) or \
            PyByteArray_Check(obj) or PyMemoryView_Check(obj)


cdef inline _is_container(object obj):
    return not _is_trivial_container(obj) and isinstance(obj, ContainerABC)


cdef inline _is_sub_array(object obj):
    return not _is_trivial_container(obj) and isinstance(obj, ContainerABC) \
            and not cpython.PyTuple_Check(obj)


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
        if _is_sub_array(elem):
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

    if not _is_container(obj):
        raise TypeError(
            'a non-trivial iterable expected (got type {!r})'.format(
                type(obj).__name__))

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


cdef inline array_decode(ConnectionSettings settings, FastReadBuffer buf,
                         decode_func_ex decoder, const void *decoder_arg):
    cdef:
        int32_t ndims = hton.unpack_int32(buf.read(4))
        int32_t flags = hton.unpack_int32(buf.read(4))
        uint32_t elem_oid = hton.unpack_int32(buf.read(4))
        list result
        uint32_t i
        int32_t elem_len
        int64_t elem_count = 1
        FastReadBuffer elem_buf = FastReadBuffer.new()
        int32_t dims[ARRAY_MAXDIM]
        Codec elem_codec

    if ndims == 0:
        result = cpython.PyList_New(0)
        return result

    if ndims > ARRAY_MAXDIM:
        raise RuntimeError(
            'number of array dimensions ({}) exceed the maximum expected ({})'.
            format(ndims, ARRAY_MAXDIM))

    if decoder == NULL:
        # No decoder is known beforehand, look it up

        elem_codec = settings.get_data_codec(elem_oid)
        if elem_codec is None or not elem_codec.has_decoder():
            raise RuntimeError(
                'no decoder for type OID {}'.format(elem_oid))

        decoder = codec_decode_func_ex
        decoder_arg = <void*>(<cpython.PyObject>elem_codec)

    for i in range(ndims):
        dims[i] = hton.unpack_int32(buf.read(4))
        elem_count *= dims[i]
        # Ignore the lower bound information
        buf.read(4)

    if ndims == 1:
        # Fast path for flat arrays
        result = cpython.PyList_New(elem_count)

        for i in range(elem_count):
            elem_len = hton.unpack_int32(buf.read(4))
            if elem_len == -1:
                elem = None
            else:
                elem_buf.slice_from(buf, elem_len)
                elem = decoder(settings, elem_buf, decoder_arg)

            cpython.Py_INCREF(elem)
            cpython.PyList_SET_ITEM(result, i, elem)

    else:
        result = _nested_array_decode(settings, buf,
                                      decoder, decoder_arg, ndims, dims,
                                      elem_buf)

    return result


cdef inline _nested_array_decode(ConnectionSettings settings,
                                 FastReadBuffer buf,
                                 decode_func_ex decoder,
                                 const void *decoder_arg,
                                 int32_t ndims, int32_t *dims,
                                 FastReadBuffer elem_buf):

    cdef:
        int32_t elem_len
        int32_t d1, d2, d3, d4, d5, d6
        list result
        object elem
        list stride1, stride2, stride3, stride4, stride5

    # Nested array.  The approach here is dumb, but fast: rely
    # on the dimension limit and shape data using nested loops.
    # Alas, Cython doesn't have preprocessor macros.
    #
    result = cpython.PyList_New(dims[0])

    for d1 in range(dims[0]):
        stride1 = cpython.PyList_New(dims[1])
        cpython.Py_INCREF(stride1)
        cpython.PyList_SET_ITEM(result, d1, stride1)

        for d2 in range(dims[1]):
            if ndims == 2:
                elem_len = hton.unpack_int32(buf.read(4))
                if elem_len == -1:
                    elem = None
                else:
                    elem = decoder(settings,
                                   elem_buf.slice_from(buf, elem_len),
                                   decoder_arg)

                cpython.Py_INCREF(elem)
                cpython.PyList_SET_ITEM(stride1, d2, elem)

            else:
                stride2 = cpython.PyList_New(dims[2])
                cpython.Py_INCREF(stride2)
                cpython.PyList_SET_ITEM(stride1, d2, stride2)

                for d3 in range(dims[2]):
                    if ndims == 3:
                        elem_len = hton.unpack_int32(buf.read(4))
                        if elem_len == -1:
                            elem = None
                        else:
                            elem = decoder(settings,
                                           elem_buf.slice_from(buf, elem_len),
                                           decoder_arg)

                        cpython.Py_INCREF(elem)
                        cpython.PyList_SET_ITEM(stride2, d3, elem)

                    else:
                        stride3 = cpython.PyList_New(dims[3])
                        cpython.Py_INCREF(stride3)
                        cpython.PyList_SET_ITEM(stride2, d3, stride3)

                        for d4 in range(dims[3]):
                            if ndims == 4:
                                elem_len = hton.unpack_int32(buf.read(4))
                                if elem_len == -1:
                                    elem = None
                                else:
                                    elem = decoder(settings,
                                                   elem_buf.slice_from(buf, elem_len),
                                                   decoder_arg)

                                cpython.Py_INCREF(elem)
                                cpython.PyList_SET_ITEM(stride3, d4, elem)

                            else:
                                stride4 = cpython.PyList_New(dims[4])
                                cpython.Py_INCREF(stride4)
                                cpython.PyList_SET_ITEM(stride3, d4, stride4)

                                for d5 in range(dims[4]):
                                    if ndims == 5:
                                        elem_len = hton.unpack_int32(buf.read(4))
                                        if elem_len == -1:
                                            elem = None
                                        else:
                                            elem = decoder(settings,
                                                           elem_buf.slice_from(buf, elem_len),
                                                           decoder_arg)

                                        cpython.Py_INCREF(elem)
                                        cpython.PyList_SET_ITEM(stride4, d5, elem)

                                    else:
                                        stride5 = cpython.PyList_New(dims[5])
                                        cpython.Py_INCREF(stride5)
                                        cpython.PyList_SET_ITEM(stride4, d5, stride5)

                                        for d6 in range(dims[5]):
                                            elem_len = hton.unpack_int32(buf.read(4))
                                            if elem_len == -1:
                                                elem = None
                                            else:
                                                elem = decoder(settings,
                                                               elem_buf.slice_from(buf, elem_len),
                                                               decoder_arg)

                                            cpython.Py_INCREF(elem)
                                            cpython.PyList_SET_ITEM(stride5, d6, elem)

    return result


cdef int4_encode_ex(ConnectionSettings settings, WriteBuffer buf, object obj,
                    const void *arg):
    return int4_encode(settings, buf, obj)


cdef int4_decode_ex(ConnectionSettings settings, FastReadBuffer buf,
                    const void *arg):
    return int4_decode(settings, buf)


cdef arrayoid_encode(ConnectionSettings settings, WriteBuffer buf, items):
    array_encode(settings, buf, items, OIDOID,
                 <encode_func_ex>&int4_encode_ex, NULL)


cdef arrayoid_decode(ConnectionSettings settings, FastReadBuffer buf):
    return array_decode(settings, buf, <decode_func_ex>&int4_decode_ex, NULL)


cdef text_encode_ex(ConnectionSettings settings, WriteBuffer buf, object obj,
                    const void *arg):
    return text_encode(settings, buf, obj)


cdef text_decode_ex(ConnectionSettings settings, FastReadBuffer buf,
                    const void *arg):
    return text_decode(settings, buf)


cdef arraytext_encode(ConnectionSettings settings, WriteBuffer buf, items):
    array_encode(settings, buf, items, TEXTOID,
                 <encode_func_ex>&text_encode_ex, NULL)


cdef arraytext_decode(ConnectionSettings settings, FastReadBuffer buf):
    return array_decode(settings, buf, <decode_func_ex>&text_decode_ex, NULL)


cdef anyarray_decode(ConnectionSettings settings, FastReadBuffer buf):
    return array_decode(settings, buf, NULL, NULL)

cdef arrayaclitemid_decode(ConnectionSettings settings, FastReadBuffer buf):

    cdef:
        # object array_text
        unicode array_text
        Py_ssize_t array_textlen
        Py_ssize_t bgn_idx
        Py_ssize_t cur_idx
        list result
        list escaped_parts
        object elem

    # Postgres does not have a function to send aclitems in binary mode
    # (aclitem_send) and arrays also must be sent in a text mode.
    # Parse it manually with the next convention (since it is system info):
    # 1. Array's index must begin from 1;
    # 2. Delimeter is ','.

    array_text = text_decode(settings, buf)
    if array_text is None:
        cpython.Py_INCREF(array_text)
        return array_text

    result = cpython.PyList_New(0)

    array_textlen = len(array_text)
    if array_textlen == 0 or array_text[1] == '}':
        return result

    if array_text[0] == '[':
        idx = array_text.find('=')
        raise ValueError('invalid array bounds (must start with 1); got: {}.'
                         .format(array_text[:idx]))

    assert array_text[0] == '{'

    cur_idx = 0
    while True:
        cur_chr = array_text[cur_idx]

        if cur_chr == '}':
            assert (cur_idx + 1) == array_textlen
            break

        assert cur_chr in '{,'
        # Here should be a check for a delimeter after a delimeter. Usually
        # it (empty string) means NULL, but for aclitem it is not possible.
        # So do nothing and get the next char
        cur_idx += 1

        bgn_idx = cur_idx
        cur_chr = array_text[cur_idx]

        if cur_chr != '"':
            # unquoted element
            cur_idx += 1
            while array_text[cur_idx] not in ',}':
                cur_idx += 1
            elem = array_text[bgn_idx:cur_idx]
            cpython.Py_INCREF(elem)
            cpython.PyList_Append(result, elem)
            continue

        # quoted element
        escaped_parts = cpython.PyList_New(0)
        cur_idx += 1
        bgn_idx = cur_idx
        while True:
            while array_text[cur_idx] not in '\\"':
                cur_idx += 1

            if array_text[cur_idx] == '"':
                # Closing quote symbol
                elem = array_text[bgn_idx:cur_idx]
                cpython.PyList_Append(escaped_parts, elem)
                break

            # Escape symbol '\'.
            # Add previous non-empty block and be ready for the next one.
            if bgn_idx != cur_idx:
                elem = array_text[bgn_idx:cur_idx]
                cpython.PyList_Append(escaped_parts, elem)

            cur_idx += 1
            bgn_idx = cur_idx # the next block begins from the escaped char

            cur_idx += 1

        elem = ''.join(escaped_parts)
        cpython.Py_INCREF(elem)
        cpython.PyList_Append(result, elem)
        cur_idx += 1  # skip the last double quote symbol

    return result

cdef arrayaclitemid_encode(ConnectionSettings settings, WriteBuffer buf, items):
    raise NotImplementedError("use postgresql's conversion "
                              "from text[] to aclitem[]")

cdef init_array_codecs():
    register_core_codec(ANYARRAYOID,
                        NULL,
                        <decode_func>&anyarray_decode,
                        PG_FORMAT_BINARY)

    # oid[] and text[] are registered as core codecs
    # to make type introspection query work
    #
    register_core_codec(_OIDOID,
                        <encode_func>&arrayoid_encode,
                        <decode_func>&arrayoid_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(_TEXTOID,
                        <encode_func>&arraytext_encode,
                        <decode_func>&arraytext_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(_ACLITEMOID,
                        <encode_func>&arrayaclitemid_encode,
                        <decode_func>&arrayaclitemid_decode,
                        PG_FORMAT_TEXT)

init_array_codecs()
