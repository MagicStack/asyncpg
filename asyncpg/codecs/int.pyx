from . cimport hton


cdef bool_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    if not cpython.PyBool_Check(obj):
        raise ValueError('invalid input for BOOLEAN type')

    buf.write_int32(1)
    buf.write_byte(b'\x01' if obj is True else b'\x00')


cdef bool_decode(ConnectionSettings settings, const char* data, int32_t len):
    return data[0] is b'\x01'


cdef int2_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef long val = cpython.PyLong_AsLong(obj)
    if val < -32767 or val > 32767:
        raise ValueError('integer too large to be encoded as INT2')

    buf.write_int32(2)
    buf.write_int16(val)


cdef int2_decode(ConnectionSettings settings, const char* data, int32_t len):
    return cpython.PyLong_FromLong(hton.unpack_int16(data))


cdef int4_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef long val = cpython.PyLong_AsLong(obj)

    buf.write_int32(4)
    buf.write_int32(val)


cdef int4_decode(ConnectionSettings settings, const char* data, int32_t len):
    return cpython.PyLong_FromLong(hton.unpack_int32(data))


cdef int8_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef long long val = cpython.PyLong_AsLongLong(obj)
    buf.write_int32(8)
    buf.write_int64(val)


cdef int8_decode(ConnectionSettings settings, const char* data, int32_t len):
    return cpython.PyLong_FromLongLong(hton.unpack_int64(data))


cdef inline void init_int_codecs():
    codec_map[BOOLOID].encode = bool_encode
    codec_map[BOOLOID].decode = bool_decode
    codec_map[INT2OID].encode = int2_encode
    codec_map[INT2OID].decode = int2_decode
    codec_map[INT4OID].encode = int4_encode
    codec_map[INT4OID].decode = int4_decode
    codec_map[INT8OID].encode = int8_encode
    codec_map[INT8OID].decode = int8_decode
