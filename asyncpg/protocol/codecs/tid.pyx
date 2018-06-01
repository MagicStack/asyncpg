# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef tid_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef int overflow = 0
    cdef unsigned long block, offset

    if not (cpython.PyTuple_Check(obj) or cpython.PyList_Check(obj)):
        raise TypeError(
            'list or tuple expected (got type {})'.format(type(obj)))

    if len(obj) != 2:
        raise ValueError(
            'invalid number of elements in tid tuple, expecting 2')

    try:
        block = cpython.PyLong_AsUnsignedLong(obj[0])
    except OverflowError:
        overflow = 1

    # "long" and "long long" have the same size for x86_64, need an extra check
    if overflow or (sizeof(block) > 4 and block > UINT32_MAX):
        raise OverflowError('tuple id block value out of uint32 range')

    try:
        offset = cpython.PyLong_AsUnsignedLong(obj[1])
        overflow = 0
    except OverflowError:
        overflow = 1

    if overflow or offset > 65535:
        raise OverflowError('tuple id offset value out of uint16 range')

    buf.write_int32(6)
    buf.write_int32(<int32_t>block)
    buf.write_int16(<int16_t>offset)


cdef tid_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        uint32_t block
        uint16_t offset

    block = <uint32_t>hton.unpack_int32(buf.read(4))
    offset = <uint16_t>hton.unpack_int16(buf.read(2))

    return (block, offset)


cdef init_tid_codecs():
    register_core_codec(TIDOID,
                        <encode_func>&tid_encode,
                        <decode_func>&tid_decode,
                        PG_FORMAT_BINARY)


init_tid_codecs()
