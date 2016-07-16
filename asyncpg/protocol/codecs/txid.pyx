# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef txid_snapshot_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        int32_t nxip
        int64_t xmin
        int64_t xmax
        int32_t i
        WriteBuffer xip_buf = WriteBuffer.new()

    if not (cpython.PyTuple_Check(obj) or cpython.PyList_Check(obj)):
        raise TypeError(
            'list or tuple expected (got type {})'.format(type(obj)))

    if len(obj) != 3:
        raise ValueError(
            'invalid number of elements in txid_snapshot tuple, expecting 4')

    nxip = len(obj[2])
    xmin = obj[0]
    xmax = obj[1]

    for i in range(nxip):
        xip_buf.write_int64(obj[2][i])

    buf.write_int32(20 + xip_buf.len())

    buf.write_int32(nxip)
    buf.write_int64(obj[0])
    buf.write_int64(obj[1])
    buf.write_buffer(xip_buf)


cdef txid_snapshot_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int32_t nxip
        int64_t xmin
        int64_t xmax
        tuple xip_tup
        int32_t i
        object xip

    nxip = hton.unpack_int32(buf.read(4))
    xmin = hton.unpack_int64(buf.read(8))
    xmax = hton.unpack_int64(buf.read(8))

    xip_tup = cpython.PyTuple_New(nxip)
    for i in range(nxip):
        xip = cpython.PyLong_FromLongLong(hton.unpack_int64(buf.read(8)))
        cpython.Py_INCREF(xip)
        cpython.PyTuple_SET_ITEM(xip_tup, i, xip)

    return (xmin, xmax, xip_tup)


cdef init_txid_codecs():
    register_core_codec(TXID_SNAPSHOTOID,
                        <encode_func>&txid_snapshot_encode,
                        <decode_func>&txid_snapshot_decode,
                        PG_FORMAT_BINARY)


init_txid_codecs()
