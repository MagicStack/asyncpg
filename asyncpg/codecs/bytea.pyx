import decimal

_Dec = decimal.Decimal


cdef bytea_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    cdef:
        Py_buffer pybuf
        bint pybuf_used = False
        char *buf
        ssize_t len

    if cpython.PyBytes_CheckExact(obj):
        buf = cpython.PyBytes_AS_STRING(obj)
        len = cpython.Py_SIZE(obj)
    else:
        cpython.PyObject_GetBuffer(obj, &pybuf, cpython.PyBUF_SIMPLE)
        pybuf_used = True
        buf = <char*>pybuf.buf
        len = pybuf.len

    try:
        wbuf.write_int32(<int32_t>len)
        wbuf.write_cstr(buf, len)
    finally:
        if pybuf_used:
            cpython.PyBuffer_Release(&pybuf)


cdef bytea_decode(ConnectionSettings settings, const char* data, int32_t len):
    return cpython.PyBytes_FromStringAndSize(data, len)


cdef inline void init_bytea_codecs():
    codec_map[BYTEAOID].encode = bytea_encode
    codec_map[BYTEAOID].decode = bytea_decode
    codec_map[BYTEAOID].format = PG_FORMAT_BINARY
