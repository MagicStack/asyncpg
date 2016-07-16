# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from libc cimport math


cdef float4_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef double dval = cpython.PyFloat_AsDouble(obj)
    cdef float fval = <float>dval
    if math.isinf(fval) and not math.isinf(dval):
        raise ValueError('float value too large to be encoded as FLOAT4')

    buf.write_int32(4)
    buf.write_float(fval)


cdef float4_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef float f = hton.unpack_float(buf.read(4))
    return cpython.PyFloat_FromDouble(f)


cdef float8_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef double dval = cpython.PyFloat_AsDouble(obj)
    buf.write_int32(8)
    buf.write_double(dval)


cdef float8_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef double f = hton.unpack_double(buf.read(8))
    return cpython.PyFloat_FromDouble(f)


cdef init_float_codecs():
    register_core_codec(FLOAT4OID,
                        <encode_func>&float4_encode,
                        <decode_func>&float4_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(FLOAT8OID,
                        <encode_func>&float8_encode,
                        <decode_func>&float8_decode,
                        PG_FORMAT_BINARY)


init_float_codecs()
