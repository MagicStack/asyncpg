# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from asyncpg.types import BitString


cdef bits_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    cdef:
        Py_buffer pybuf
        bint pybuf_used = False
        char *buf
        ssize_t len
        int32_t bitlen

    if cpython.PyBytes_CheckExact(obj):
        buf = cpython.PyBytes_AS_STRING(obj)
        len = cpython.Py_SIZE(obj)
        bitlen = len * 8
    elif isinstance(obj, BitString):
        cpython.PyBytes_AsStringAndSize(obj.bytes, &buf, &len)
        bitlen = obj.__len__()
    else:
        cpython.PyObject_GetBuffer(obj, &pybuf, cpython.PyBUF_SIMPLE)
        pybuf_used = True
        buf = <char*>pybuf.buf
        len = pybuf.len
        bitlen = len * 8

    try:
        wbuf.write_int32(4 + len)
        wbuf.write_int32(<int32_t>bitlen)
        wbuf.write_cstr(buf, len)
    finally:
        if pybuf_used:
            cpython.PyBuffer_Release(&pybuf)


cdef bits_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int32_t bitlen = hton.unpack_int32(buf.read(4))
        size_t buf_len = buf.len

    bytes_ = cpython.PyBytes_FromStringAndSize(buf.read_all(), buf_len)
    return BitString.frombytes(bytes_, bitlen)


cdef init_bits_codecs():
    register_core_codec(BITOID,
                        <encode_func>&bits_encode,
                        <decode_func>&bits_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(VARBITOID,
                        <encode_func>&bits_encode,
                        <decode_func>&bits_decode,
                        PG_FORMAT_BINARY)

init_bits_codecs()
