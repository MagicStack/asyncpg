from libc cimport math
from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t


from . cimport hton


cdef class ConnectionSettings:
    pass


cdef void bool_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    if not cpython.PyBool_Check(obj):
        raise ValueError('invalid input for BOOLEAN type')

    buf.write_byte(b'\x01' if obj is True else b'\x00')


cdef bool_decode(ConnectionSettings settings, bytes data):
    return data[0] is b'\x01'


cdef void int2_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef long val = cpython.PyLong_AsLong(obj)
    if val < -32767 or val > 32767:
        raise ValueError('integer too large to be encoded as INT2')

    buf.write_int16(val)


cdef int2_decode(ConnectionSettings settings, const char* data):
    return cpython.PyLong_FromLong(hton.unpack_int16(data))


cdef void int4_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef long val = cpython.PyLong_AsLong(obj)
    buf.write_int32(val)


cdef int4_decode(ConnectionSettings settings, const char* data):
    return cpython.PyLong_FromLong(hton.unpack_int32(data))


cdef void int8_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef long long val = cpython.PyLong_AsLongLong(obj)
    buf.write_int64(val)


cdef int8_decode(ConnectionSettings settings, const char* data):
    return cpython.PyLong_FromLongLong(hton.unpack_int64(data))


cdef union _floatconv:
    uint32_t i
    float f


cdef void float4_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef double dval = cpython.PyFloat_AsDouble(obj)
    cdef float fval = <float>dval
    if math.isinf(fval) and not math.isinf(dval):
        raise ValueError('float value too large to be encoded as FLOAT4')

    cdef _floatconv v

    v.f = fval

    buf.write_int32(v.i)


cdef float4_decode(ConnectionSettings settings, const char* data):
    cdef _floatconv v
    v.i = hton.unpack_int32(data)

    return cpython.PyFloat_FromDouble(v.f)


cdef union _doubleconv:
    uint64_t i
    double f


cdef void float8_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef double dval = cpython.PyFloat_AsDouble(obj)
    cdef _doubleconv v

    v.f = dval

    buf.write_int64(v.i)


cdef float8_decode(ConnectionSettings settings, const char* data):
    cdef _doubleconv v
    v.i = hton.unpack_int64(data)

    return cpython.PyFloat_FromDouble(v.f)
