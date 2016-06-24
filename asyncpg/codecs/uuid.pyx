import uuid

_UUID = uuid.UUID


cdef uuid_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    cdef:
        Py_buffer pybuf
        bint pybuf_used = False
        char *buf
        ssize_t len

    if cpython.PyUnicode_Check(obj):
        obj = _UUID(obj)

    bytea_encode(settings, wbuf, obj.bytes)


cdef uuid_decode(ConnectionSettings settings, const char* data, int32_t len):
    return _UUID(bytes=cpython.PyBytes_FromStringAndSize(data, len))


cdef init_uuid_codecs():
    register_core_codec(UUIDOID,
                        <encode_func>&uuid_encode,
                        <decode_func>&uuid_decode,
                        PG_FORMAT_BINARY)
