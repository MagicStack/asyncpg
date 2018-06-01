# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cimport cpython.datetime
import datetime

cpython.datetime.import_datetime()

utc = datetime.timezone.utc
date_from_ordinal = datetime.date.fromordinal
timedelta = datetime.timedelta

pg_epoch_datetime = datetime.datetime(2000, 1, 1)
cdef int32_t pg_epoch_datetime_ts = \
    <int32_t>cpython.PyLong_AsLong(int(pg_epoch_datetime.timestamp()))

pg_epoch_datetime_utc = datetime.datetime(2000, 1, 1, tzinfo=utc)
cdef int32_t pg_epoch_datetime_utc_ts = \
    <int32_t>cpython.PyLong_AsLong(pg_epoch_datetime_utc.timestamp())

pg_epoch_date = datetime.date(2000, 1, 1)
cdef int32_t pg_date_offset_ord = \
    <int32_t>cpython.PyLong_AsLong(pg_epoch_date.toordinal())

# Binary representations of infinity for datetimes.
cdef int64_t pg_time64_infinity = 0x7fffffffffffffff
cdef int64_t pg_time64_negative_infinity = <int64_t>0x8000000000000000
cdef int32_t pg_date_infinity = 0x7fffffff
cdef int32_t pg_date_negative_infinity = <int32_t>0x80000000

infinity_datetime = datetime.datetime(
    datetime.MAXYEAR, 12, 31, 23, 59, 59, 999999)

cdef int32_t infinity_datetime_ord = <int32_t>cpython.PyLong_AsLong(
    infinity_datetime.toordinal())

cdef int64_t infinity_datetime_ts = 252455615999999999

negative_infinity_datetime = datetime.datetime(
    datetime.MINYEAR, 1, 1, 0, 0, 0, 0)

cdef int32_t negative_infinity_datetime_ord = <int32_t>cpython.PyLong_AsLong(
    negative_infinity_datetime.toordinal())

cdef int64_t negative_infinity_datetime_ts = -63082281600000000

infinity_date = datetime.date(datetime.MAXYEAR, 12, 31)

cdef int32_t infinity_date_ord = <int32_t>cpython.PyLong_AsLong(
    infinity_date.toordinal())

negative_infinity_date = datetime.date(datetime.MINYEAR, 1, 1)

cdef int32_t negative_infinity_date_ord = <int32_t>cpython.PyLong_AsLong(
    negative_infinity_date.toordinal())


cdef inline _local_timezone():
    d = datetime.datetime.now(datetime.timezone.utc).astimezone()
    return datetime.timezone(d.utcoffset())


cdef inline _encode_time(WriteBuffer buf, int64_t seconds,
                         int32_t microseconds):
    # XXX: add support for double timestamps
    # int64 timestamps,
    cdef int64_t ts = seconds * 1000000 + microseconds

    if ts == infinity_datetime_ts:
        buf.write_int64(pg_time64_infinity)
    elif ts == negative_infinity_datetime_ts:
        buf.write_int64(pg_time64_negative_infinity)
    else:
        buf.write_int64(ts)


cdef inline int32_t _decode_time(FastReadBuffer buf, int64_t *seconds,
                                 uint32_t *microseconds):
    # XXX: add support for double timestamps
    # int64 timestamps,
    cdef int64_t ts = hton.unpack_int64(buf.read(8))

    if ts == pg_time64_infinity:
        return 1
    elif ts == pg_time64_negative_infinity:
        return -1

    seconds[0] = <int64_t>(ts / 1000000)
    microseconds[0] = <uint32_t>(ts % 1000000)

    return 0


cdef date_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        int32_t ordinal = <int32_t>cpython.PyLong_AsLong(obj.toordinal())
        int32_t pg_ordinal

    if ordinal == infinity_date_ord:
        pg_ordinal = pg_date_infinity
    elif ordinal == negative_infinity_date_ord:
        pg_ordinal = pg_date_negative_infinity
    else:
        pg_ordinal = ordinal - pg_date_offset_ord

    buf.write_int32(4)
    buf.write_int32(pg_ordinal)


cdef date_encode_tuple(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        int32_t pg_ordinal

    if len(obj) != 1:
        raise ValueError(
            'date tuple encoder: expecting 1 element '
            'in tuple, got {}'.format(len(obj)))

    pg_ordinal = obj[0]
    buf.write_int32(4)
    buf.write_int32(pg_ordinal)


cdef date_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef int32_t pg_ordinal = hton.unpack_int32(buf.read(4))

    if pg_ordinal == pg_date_infinity:
        return infinity_date
    elif pg_ordinal == pg_date_negative_infinity:
        return negative_infinity_date
    else:
        return date_from_ordinal(pg_ordinal + pg_date_offset_ord)


cdef date_decode_tuple(ConnectionSettings settings, FastReadBuffer buf):
    cdef int32_t pg_ordinal = hton.unpack_int32(buf.read(4))

    return (pg_ordinal,)


cdef timestamp_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    if not cpython.datetime.PyDateTime_Check(obj):
        if cpython.datetime.PyDate_Check(obj):
            obj = datetime.datetime(obj.year, obj.month, obj.day)
        else:
            raise TypeError(
                'expected a datetime.date or datetime.datetime instance, '
                'got {!r}'.format(type(obj).__name__)
            )

    delta = obj - pg_epoch_datetime
    cdef:
        int64_t seconds = cpython.PyLong_AsLongLong(delta.days) * 86400 + \
                                cpython.PyLong_AsLong(delta.seconds)
        int32_t microseconds = <int32_t>cpython.PyLong_AsLong(
                                    delta.microseconds)

    buf.write_int32(8)
    _encode_time(buf, seconds, microseconds)


cdef timestamp_encode_tuple(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        int64_t microseconds

    if len(obj) != 1:
        raise ValueError(
            'timestamp tuple encoder: expecting 1 element '
            'in tuple, got {}'.format(len(obj)))

    microseconds = obj[0]

    buf.write_int32(8)
    buf.write_int64(microseconds)


cdef timestamp_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int64_t seconds = 0
        uint32_t microseconds = 0
        int32_t inf = _decode_time(buf, &seconds, &microseconds)

    if inf > 0:
        # positive infinity
        return infinity_datetime
    elif inf < 0:
        # negative infinity
        return negative_infinity_datetime
    else:
        return pg_epoch_datetime.__add__(
            timedelta(0, seconds, microseconds))


cdef timestamp_decode_tuple(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int64_t ts = hton.unpack_int64(buf.read(8))

    return (ts,)


cdef timestamptz_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    if not cpython.datetime.PyDateTime_Check(obj):
        if cpython.datetime.PyDate_Check(obj):
            obj = datetime.datetime(obj.year, obj.month, obj.day,
                                    tzinfo=_local_timezone())
        else:
            raise TypeError(
                'expected a datetime.date or datetime.datetime instance, '
                'got {!r}'.format(type(obj).__name__)
            )

    buf.write_int32(8)

    if obj == infinity_datetime:
        buf.write_int64(pg_time64_infinity)
        return
    elif obj == negative_infinity_datetime:
        buf.write_int64(pg_time64_negative_infinity)
        return

    try:
        utc_dt = obj.astimezone(utc)
    except ValueError:
        # Python 3.5 doesn't like it when we call astimezone()
        # on naive datetime objects, so make it aware.
        utc_dt = obj.replace(tzinfo=_local_timezone()).astimezone(utc)

    delta = utc_dt - pg_epoch_datetime_utc
    cdef:
        int64_t seconds = cpython.PyLong_AsLongLong(delta.days) * 86400 + \
                                cpython.PyLong_AsLong(delta.seconds)
        int32_t microseconds = <int32_t>cpython.PyLong_AsLong(
                                    delta.microseconds)

    _encode_time(buf, seconds, microseconds)


cdef timestamptz_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int64_t seconds = 0
        uint32_t microseconds = 0
        int32_t inf = _decode_time(buf, &seconds, &microseconds)

    if inf > 0:
        # positive infinity
        return infinity_datetime
    elif inf < 0:
        # negative infinity
        return negative_infinity_datetime
    else:
        return pg_epoch_datetime_utc.__add__(
            timedelta(0, seconds, microseconds))


cdef time_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        int64_t seconds = cpython.PyLong_AsLong(obj.hour) * 3600 + \
                            cpython.PyLong_AsLong(obj.minute) * 60 + \
                            cpython.PyLong_AsLong(obj.second)
        int32_t microseconds = <int32_t>cpython.PyLong_AsLong(obj.microsecond)

    buf.write_int32(8)
    _encode_time(buf, seconds, microseconds)


cdef time_encode_tuple(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        int64_t microseconds

    if len(obj) != 1:
        raise ValueError(
            'time tuple encoder: expecting 1 element '
            'in tuple, got {}'.format(len(obj)))

    microseconds = obj[0]

    buf.write_int32(8)
    buf.write_int64(microseconds)


cdef time_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int64_t seconds = 0
        uint32_t microseconds = 0

    _decode_time(buf, &seconds, &microseconds)

    cdef:
        int64_t minutes = <int64_t>(seconds / 60)
        int64_t sec = seconds % 60
        int64_t hours = <int64_t>(minutes / 60)
        int64_t min = minutes % 60

    return datetime.time(hours, min, sec, microseconds)


cdef time_decode_tuple(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int64_t ts = hton.unpack_int64(buf.read(8))

    return (ts,)


cdef timetz_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    offset = obj.tzinfo.utcoffset(None)

    cdef:
        int32_t offset_sec = \
            <int32_t>cpython.PyLong_AsLong(offset.days) * 24 * 60 * 60 + \
            <int32_t>cpython.PyLong_AsLong(offset.seconds)

        int64_t seconds = cpython.PyLong_AsLong(obj.hour) * 3600 + \
                            cpython.PyLong_AsLong(obj.minute) * 60 + \
                            cpython.PyLong_AsLong(obj.second)

        int32_t microseconds = <int32_t>cpython.PyLong_AsLong(obj.microsecond)

    buf.write_int32(12)
    _encode_time(buf, seconds, microseconds)
    # In Python utcoffset() is the difference between the local time
    # and the UTC, whereas in PostgreSQL it's the opposite,
    # so we need to flip the sign.
    buf.write_int32(-offset_sec)


cdef timetz_encode_tuple(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        int64_t microseconds
        int32_t offset_sec

    if len(obj) != 2:
        raise ValueError(
            'time tuple encoder: expecting 2 elements2 '
            'in tuple, got {}'.format(len(obj)))

    microseconds = obj[0]
    offset_sec = obj[1]

    buf.write_int32(12)
    buf.write_int64(microseconds)
    buf.write_int32(offset_sec)


cdef timetz_decode(ConnectionSettings settings, FastReadBuffer buf):
    time = time_decode(settings, buf)
    cdef int32_t offset = <int32_t>(hton.unpack_int32(buf.read(4)) / 60)
    # See the comment in the `timetz_encode` method.
    return time.replace(tzinfo=datetime.timezone(timedelta(minutes=-offset)))


cdef timetz_decode_tuple(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int64_t microseconds = hton.unpack_int64(buf.read(8))
        int32_t offset_sec = hton.unpack_int32(buf.read(4))

    return (microseconds, offset_sec)


cdef interval_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        int32_t days = <int32_t>cpython.PyLong_AsLong(obj.days)
        int64_t seconds = cpython.PyLong_AsLongLong(obj.seconds)
        int32_t microseconds = <int32_t>cpython.PyLong_AsLong(obj.microseconds)

    buf.write_int32(16)
    _encode_time(buf, seconds, microseconds)
    buf.write_int32(days)
    buf.write_int32(0) # Months


cdef interval_encode_tuple(ConnectionSettings settings, WriteBuffer buf,
                           tuple obj):
    cdef:
        int32_t months
        int32_t days
        int64_t microseconds

    if len(obj) != 3:
        raise ValueError(
            'interval tuple encoder: expecting 3 elements '
            'in tuple, got {}'.format(len(obj)))

    months = obj[0]
    days = obj[1]
    microseconds = obj[2]

    buf.write_int32(16)
    buf.write_int64(microseconds)
    buf.write_int32(days)
    buf.write_int32(months)


cdef interval_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int32_t days
        int32_t months
        int32_t years
        int64_t seconds = 0
        uint32_t microseconds = 0

    _decode_time(buf, &seconds, &microseconds)
    days = hton.unpack_int32(buf.read(4))
    months = hton.unpack_int32(buf.read(4))

    if months < 0:
        years = -<int32_t>(-months // 12)
        months = -<int32_t>(-months % 12)
    else:
        years = <int32_t>(months // 12)
        months = <int32_t>(months % 12)

    return datetime.timedelta(days=days + months * 30 + years * 365,
                              seconds=seconds, microseconds=microseconds)


cdef interval_decode_tuple(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int32_t days
        int32_t months
        int64_t microseconds

    microseconds = hton.unpack_int64(buf.read(8))
    days = hton.unpack_int32(buf.read(4))
    months = hton.unpack_int32(buf.read(4))

    return (months, days, microseconds)


cdef init_datetime_codecs():
    register_core_codec(DATEOID,
                        <encode_func>&date_encode,
                        <decode_func>&date_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(DATEOID,
                        <encode_func>&date_encode_tuple,
                        <decode_func>&date_decode_tuple,
                        PG_FORMAT_BINARY,
                        PG_XFORMAT_TUPLE)

    register_core_codec(TIMEOID,
                        <encode_func>&time_encode,
                        <decode_func>&time_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(TIMEOID,
                        <encode_func>&time_encode_tuple,
                        <decode_func>&time_decode_tuple,
                        PG_FORMAT_BINARY,
                        PG_XFORMAT_TUPLE)

    register_core_codec(TIMETZOID,
                        <encode_func>&timetz_encode,
                        <decode_func>&timetz_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(TIMETZOID,
                        <encode_func>&timetz_encode_tuple,
                        <decode_func>&timetz_decode_tuple,
                        PG_FORMAT_BINARY,
                        PG_XFORMAT_TUPLE)

    register_core_codec(TIMESTAMPOID,
                        <encode_func>&timestamp_encode,
                        <decode_func>&timestamp_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(TIMESTAMPOID,
                        <encode_func>&timestamp_encode_tuple,
                        <decode_func>&timestamp_decode_tuple,
                        PG_FORMAT_BINARY,
                        PG_XFORMAT_TUPLE)

    register_core_codec(TIMESTAMPTZOID,
                        <encode_func>&timestamptz_encode,
                        <decode_func>&timestamptz_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(TIMESTAMPTZOID,
                        <encode_func>&timestamp_encode_tuple,
                        <decode_func>&timestamp_decode_tuple,
                        PG_FORMAT_BINARY,
                        PG_XFORMAT_TUPLE)

    register_core_codec(INTERVALOID,
                        <encode_func>&interval_encode,
                        <decode_func>&interval_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(INTERVALOID,
                        <encode_func>&interval_encode_tuple,
                        <decode_func>&interval_decode_tuple,
                        PG_FORMAT_BINARY,
                        PG_XFORMAT_TUPLE)

    # For obsolete abstime/reltime/tinterval, we do not bother to
    # interpret the value, and simply return and pass it as text.
    #
    register_core_codec(ABSTIMEOID,
                        <encode_func>&text_encode,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)

    register_core_codec(RELTIMEOID,
                        <encode_func>&text_encode,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)

    register_core_codec(TINTERVALOID,
                        <encode_func>&text_encode,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)

init_datetime_codecs()
