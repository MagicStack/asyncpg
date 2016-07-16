# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from asyncpg.types import Box, Line, LineSegment, Path, Point, Polygon, Circle


cdef inline _encode_points(WriteBuffer wbuf, object points):
    cdef object point

    for point in points:
        wbuf.write_double(point[0])
        wbuf.write_double(point[1])


cdef inline _decode_points(FastReadBuffer buf):
    cdef:
        int32_t npts = hton.unpack_int32(buf.read(4))
        pts = cpython.PyTuple_New(npts)
        int32_t i
        object point
        double x
        double y

    for i in range(npts):
        x = hton.unpack_double(buf.read(8))
        y = hton.unpack_double(buf.read(8))
        point = Point(x, y)
        cpython.Py_INCREF(point)
        cpython.PyTuple_SET_ITEM(pts, i, point)

    return pts


cdef box_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    wbuf.write_int32(32)
    _encode_points(wbuf, (obj[0], obj[1]))


cdef box_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        double high_x = hton.unpack_double(buf.read(8))
        double high_y = hton.unpack_double(buf.read(8))
        double low_x = hton.unpack_double(buf.read(8))
        double low_y = hton.unpack_double(buf.read(8))

    return Box(Point(high_x, high_y), Point(low_x, low_y))


cdef line_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    wbuf.write_int32(24)
    wbuf.write_double(obj[0])
    wbuf.write_double(obj[1])
    wbuf.write_double(obj[2])


cdef line_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        double A = hton.unpack_double(buf.read(8))
        double B = hton.unpack_double(buf.read(8))
        double C = hton.unpack_double(buf.read(8))

    return Line(A, B, C)


cdef lseg_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    wbuf.write_int32(32)
    _encode_points(wbuf, (obj[0], obj[1]))


cdef lseg_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        double p1_x = hton.unpack_double(buf.read(8))
        double p1_y = hton.unpack_double(buf.read(8))
        double p2_x = hton.unpack_double(buf.read(8))
        double p2_y = hton.unpack_double(buf.read(8))

    return LineSegment((p1_x, p1_y), (p2_x, p2_y))


cdef point_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    wbuf.write_int32(16)
    wbuf.write_double(obj[0])
    wbuf.write_double(obj[1])


cdef point_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        double x = hton.unpack_double(buf.read(8))
        double y = hton.unpack_double(buf.read(8))

    return Point(x, y)


cdef path_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    cdef:
        bint is_closed
        int32_t npts
        int32_t i

    if cpython.PyTuple_Check(obj):
        is_closed = 1
    elif cpython.PyList_Check(obj):
        is_closed = 0
    elif isinstance(obj, Path):
        is_closed = obj.is_closed

    npts = len(obj)

    wbuf.write_int32(1 + 4 + 16 * npts)

    wbuf.write_byte(is_closed)
    wbuf.write_int32(npts)

    _encode_points(wbuf, obj)


cdef path_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int8_t is_closed = <int8_t>buf.read(1)[0]

    return Path(*_decode_points(buf), is_closed=is_closed == 1)


cdef poly_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    cdef:
        bint is_closed
        int32_t npts
        int32_t i

    npts = len(obj)

    wbuf.write_int32(4 + 16 * npts)
    wbuf.write_int32(npts)
    _encode_points(wbuf, obj)


cdef poly_decode(ConnectionSettings settings, FastReadBuffer buf):
    return Polygon(*_decode_points(buf))


cdef circle_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    wbuf.write_int32(24)
    wbuf.write_double(obj[0][0])
    wbuf.write_double(obj[0][1])
    wbuf.write_double(obj[1])


cdef circle_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        double center_x = hton.unpack_double(buf.read(8))
        double center_y = hton.unpack_double(buf.read(8))
        double radius = hton.unpack_double(buf.read(8))

    return Circle((center_x, center_y), radius)


cdef init_geometry_codecs():
    register_core_codec(BOXOID,
                        <encode_func>&box_encode,
                        <decode_func>&box_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(LINEOID,
                        <encode_func>&line_encode,
                        <decode_func>&line_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(LSEGOID,
                        <encode_func>&lseg_encode,
                        <decode_func>&lseg_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(POINTOID,
                        <encode_func>&point_encode,
                        <decode_func>&point_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(PATHOID,
                        <encode_func>&path_encode,
                        <decode_func>&path_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(POLYGONOID,
                        <encode_func>&poly_encode,
                        <decode_func>&poly_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(CIRCLEOID,
                        <encode_func>&circle_encode,
                        <decode_func>&circle_decode,
                        PG_FORMAT_BINARY)


init_geometry_codecs()
