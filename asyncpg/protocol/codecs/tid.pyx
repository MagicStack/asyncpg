# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef tid_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef int overflow = 0
    cdef long block, offset

    if not (cpython.PyTuple_Check(obj) or cpython.PyList_Check(obj)):
        raise TypeError(
            'list or tuple expected (got type {})'.format(type(obj)))

    if len(obj) != 2:
        raise ValueError(
            'invalid number of elements in tid tuple, expecting 2')

    try:
        block = cpython.PyLong_AsLong(obj[0])
    except OverflowError:
        overflow = 1

    # "long" and "long long" have the same size for x86_64, need an extra check
    if overflow or (sizeof(block) > 4 and (block < -2147483648 or
                                                 block > 2147483647)):
        raise OverflowError(
            'block too big to be encoded as INT4: {!r}'.format(obj[0]))

    try:
        offset = cpython.PyLong_AsLong(obj[1])
        overflow = 0
    except OverflowError:
        overflow = 1

    if overflow or offset < -32768 or offset > 32767:
        raise OverflowError(
            'offset too big to be encoded as INT2: {!r}'.format(obj[1]))

    buf.write_int32(6)
    buf.write_int32(<int32_t>block)
    buf.write_int16(<int16_t>offset)


cdef tid_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int32_t block
        int16_t offset

    block = hton.unpack_int32(buf.read(4))
    offset = hton.unpack_int16(buf.read(2))

    return (block, offset)


cdef init_tid_codecs():
    register_core_codec(TIDOID,
                        <encode_func>&tid_encode,
                        <decode_func>&tid_decode,
                        PG_FORMAT_BINARY)


init_tid_codecs()
