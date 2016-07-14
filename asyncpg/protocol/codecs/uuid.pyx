import uuid


_UUID = uuid.UUID


cdef uuid_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    if cpython.PyUnicode_Check(obj):
        obj = _UUID(obj)

    bytea_encode(settings, wbuf, obj.bytes)


cdef uuid_decode(ConnectionSettings settings, FastReadBuffer buf):
    return _UUID(bytes=bytea_decode(settings, buf))


cdef init_uuid_codecs():
    register_core_codec(UUIDOID,
                        <encode_func>&uuid_encode,
                        <decode_func>&uuid_decode,
                        PG_FORMAT_BINARY)

init_uuid_codecs()
