from libc cimport math

cdef union _floatconv:
    uint32_t i
    float f


cdef float4_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef double dval = cpython.PyFloat_AsDouble(obj)
    cdef float fval = <float>dval
    if math.isinf(fval) and not math.isinf(dval):
        raise ValueError('float value too large to be encoded as FLOAT4')

    cdef _floatconv v

    v.f = fval

    buf.write_int32(4)
    buf.write_int32(v.i)


cdef float4_decode(ConnectionSettings settings, const char* data, int32_t len):
    cdef _floatconv v
    v.i = hton.unpack_int32(data)

    return cpython.PyFloat_FromDouble(v.f)


cdef union _doubleconv:
    uint64_t i
    double f


cdef float8_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef double dval = cpython.PyFloat_AsDouble(obj)
    cdef _doubleconv v

    v.f = dval

    buf.write_int32(8)
    buf.write_int64(v.i)


cdef float8_decode(ConnectionSettings settings, const char* data, int32_t len):
    cdef _doubleconv v
    v.i = hton.unpack_int64(data)

    return cpython.PyFloat_FromDouble(v.f)


cdef init_float_codecs():

    register_core_codec(FLOAT4OID,
                        <encode_func>&float4_encode,
                        <decode_func>&float4_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(FLOAT8OID,
                        <encode_func>&float8_encode,
                        <decode_func>&float8_decode,
                        PG_FORMAT_BINARY)
