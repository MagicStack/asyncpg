# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef bool_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    if not cpython.PyBool_Check(obj):
        raise TypeError('a boolean is required (got type {})'.format(
            type(obj).__name__))

    buf.write_int32(1)
    buf.write_byte(b'\x01' if obj is True else b'\x00')


cdef bool_decode(ConnectionSettings settings, FastReadBuffer buf):
    return buf.read(1)[0] is b'\x01'


cdef int2_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef int overflow = 0
    cdef long val

    try:
        val = cpython.PyLong_AsLong(obj)
    except OverflowError:
        overflow = 1

    if overflow or val < -32768 or val > 32767:
        raise OverflowError(
            'int too big to be encoded as INT2: {!r}'.format(obj))

    buf.write_int32(2)
    buf.write_int16(<int16_t>val)


cdef int2_decode(ConnectionSettings settings, FastReadBuffer buf):
    return cpython.PyLong_FromLong(hton.unpack_int16(buf.read(2)))


cdef int4_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef int overflow = 0
    cdef long val

    try:
        val = cpython.PyLong_AsLong(obj)
    except OverflowError:
        overflow = 1

    # "long" and "long long" have the same size for x86_64, need an extra check
    if overflow or (sizeof(val) > 4 and (val < -2147483648 or
                                         val > 2147483647)):
        raise OverflowError(
            'int too big to be encoded as INT4: {!r}'.format(obj))

    buf.write_int32(4)
    buf.write_int32(<int32_t>val)


cdef int4_decode(ConnectionSettings settings, FastReadBuffer buf):
    return cpython.PyLong_FromLong(hton.unpack_int32(buf.read(4)))


cdef int8_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef int overflow = 0
    cdef long long val

    try:
        val = cpython.PyLong_AsLongLong(obj)
    except OverflowError:
        overflow = 1

    # Just in case for systems with "long long" bigger than 8 bytes
    if overflow or (sizeof(val) > 8 and (val < -9223372036854775808 or
                                         val > 9223372036854775807)):
        raise OverflowError(
            'int too big to be encoded as INT8: {!r}'.format(obj))

    buf.write_int32(8)
    buf.write_int64(<int64_t>val)


cdef int8_decode(ConnectionSettings settings, FastReadBuffer buf):
    return cpython.PyLong_FromLongLong(hton.unpack_int64(buf.read(8)))


cdef init_int_codecs():

    register_core_codec(BOOLOID,
                        <encode_func>&bool_encode,
                        <decode_func>&bool_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(INT2OID,
                        <encode_func>&int2_encode,
                        <decode_func>&int2_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(INT4OID,
                        <encode_func>&int4_encode,
                        <decode_func>&int4_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(INT8OID,
                        <encode_func>&int8_encode,
                        <decode_func>&int8_decode,
                        PG_FORMAT_BINARY)


init_int_codecs()
