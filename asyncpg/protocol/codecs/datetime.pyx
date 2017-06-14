# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import datetime

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


cdef date_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef int32_t pg_ordinal = hton.unpack_int32(buf.read(4))

    if pg_ordinal == pg_date_infinity:
        return infinity_date
    elif pg_ordinal == pg_date_negative_infinity:
        return negative_infinity_date
    else:
        return date_from_ordinal(pg_ordinal + pg_date_offset_ord)


cdef timestamp_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    delta = obj - pg_epoch_datetime
    cdef:
        int64_t seconds = cpython.PyLong_AsLongLong(delta.days) * 86400 + \
                                cpython.PyLong_AsLong(delta.seconds)
        int32_t microseconds = <int32_t>cpython.PyLong_AsLong(
                                    delta.microseconds)

    buf.write_int32(8)
    _encode_time(buf, seconds, microseconds)


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


cdef timestamptz_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    buf.write_int32(8)

    if obj == infinity_datetime:
        buf.write_int64(pg_time64_infinity)
        return
    elif obj == negative_infinity_datetime:
        buf.write_int64(pg_time64_negative_infinity)
        return

    delta = obj.astimezone(utc) - pg_epoch_datetime_utc
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
    buf.write_int32(offset_sec)


cdef timetz_decode(ConnectionSettings settings, FastReadBuffer buf):
    time = time_decode(settings, buf)
    cdef int32_t offset = <int32_t>(hton.unpack_int32(buf.read(4)) / 60)
    return time.replace(tzinfo=datetime.timezone(timedelta(minutes=offset)))


cdef int32_t is_leap(int32_t year):
    return ((year % 4) == 0 and ((year % 100) != 0 or (year % 400) == 0))


cdef int32_t month_days(int32_t year, int32_t month):
    if month == 4 or month == 6 or month == 9 or month == 11:
        return 30
    elif month == 2:
        return 29 if is_leap(year) else 28
    else:
        return 31


cdef to_timedelta(int32_t months, int32_t days, int64_t time,
                  int32_t year, int32_t month, int32_t day):
    cdef:
        int64_t cum_days
        int32_t step
        int64_t seconds
        uint32_t microseconds
        int32_t mdays

    step = -1 if months < 0 else 1
    cum_days = 0

    while months:
        months -= step
        if step == 1:
            mdays = month_days(year, month)
        month += step
        if step == 1:
            if month == 13:
                month = 1
                year += 1
        else:
            if month == 0:
                month = 12
                year -= 1
            mdays = month_days(year, month)
        cum_days += mdays

    cum_days *= step

    mdays = month_days(year, month)
    if day > mdays:
        days -= day - mdays

    cum_days += days

    seconds, microseconds = divmod(time, 1000000)
    days, seconds = divmod(seconds, 24 * 60 * 60)

    return timedelta(cum_days + days, seconds, microseconds)


cdef class Interval(object):
    cdef:
        readonly int32_t months
        readonly int32_t days
        readonly int64_t time

    def __init__(self, int32_t months, int32_t days, int64_t time=0,
                 int64_t hours=0, int64_t minutes=0, int64_t seconds=0, int64_t microseconds=0):
        if time == 0:
            carry, microseconds = divmod(microseconds, 1000000)
            carry, seconds = divmod(seconds + carry, 60)
            carry, minutes = divmod(minutes + carry, 60)
            carry, hours = divmod(hours + carry, 24)
            days += carry
            time = (hours * 60 * 60 + minutes * 60 + seconds) * 1000000 + microseconds

        self.months = months
        self.days = days
        self.time = time

    @property
    def hours(self):
        return (self.time // (60 * 60 * 1000000))

    @property
    def minutes(self):
        return (self.time // (60 * 1000000)) % 60

    @property
    def seconds(self):
        return (self.time // 1000000) % 60

    @property
    def microseconds(self):
        return self.time % 1000000

    def __repr__(self):
        return "Interval(months=%d, days=%d, time=%d)" % (self.months, self.days, self.time)

    def __str__(self):
        cdef:
            int32_t hours
            int32_t mins
            int32_t secs
            int32_t usecs
            int64_t time

        parts = []

        if self.months:
            parts.append("%d month%s" % (self.months, '' if abs(self.months) == 1 else 's'))

        if self.days:
            parts.append("%d day%s" % (self.days, '' if abs(self.days) == 1 else 's'))

        if self.time:
            time, usecs = divmod(self.time, 1000000)
            time, secs = divmod(time, 60)
            time, mins = divmod(time, 60)
            time, hours = divmod(time, 60)
            sign = '-' if time < 0 else ''
            if usecs:
                parts.append("%s%02d:%02d:%02d.%d" % (sign, hours, mins, secs, usecs))
            else:
                parts.append("%s%02d:%02d:%02d" % (sign, hours, mins, secs))

        return ', '.join(parts)

    def __hash__(self):
        return hash((self.months, self.days, self.time))

    def __richcmp__(self, other, int op):
        if isinstance(other, Interval):
            if op == 2: # ==
                return (self.months == other.months
                        and self.days == other.days
                        and self.time == other.time)
            elif op == 3: # !=
                return (self.months != other.months
                        or self.days != other.days
                        or self.time != other.time)
            elif op == 0: # <
                return ((self.months, self.days, self.time)
                        <
                        (other.months, other.days, other.time))
            elif op == 1: # <=
                return ((self.months, self.days, self.time)
                        <=
                        (other.months, other.days, other.time))
            elif op == 4: # >
                return ((self.months, self.days, self.time)
                        >
                        (other.months, other.days, other.time))
            elif op == 5: # >=
                return ((self.months, self.days, self.time)
                        >=
                        (other.months, other.days, other.time))
        return False

    def __abs__(self):
        return Interval(abs(self.months),
                        abs(self.days),
                        abs(self.time))

    def __neg__(self):
        return Interval(-self.months,
                        -self.days,
                        -self.time)

    def __add__(self, other):
        if isinstance(self, Interval):
            if isinstance(other, Interval):
                return Interval(self.months + other.months,
                                self.days + other.days,
                                self.time + other.time)

            if isinstance(other, datetime.date):
                delta = to_timedelta(self.months, self.days, self.time,
                                     other.year, other.month, other.day)

                if isinstance(other, datetime.datetime):
                    result = other
                else:
                    result = datetime.datetime(year=other.year, month=other.month, day=other.day)

                return result + delta

        elif isinstance(self, datetime.date):
            if isinstance(other, Interval):
                delta = to_timedelta(other.months, other.days, other.time,
                                     self.year, self.month, self.day)
                if isinstance(self, datetime.datetime):
                    result = self
                else:
                    result = datetime.datetime(year=self.year, month=self.month, day=self.day)

                return result + delta

        return NotImplemented

    def __sub__(self, other):
        if isinstance(self, Interval):
            if isinstance(other, Interval):
                return Interval(self.months - other.months,
                                self.days - other.days,
                                self.time - other.time)

        elif isinstance(self, datetime.date):
            if isinstance(other, Interval):
                delta = to_timedelta(-other.months, -other.days, -other.time,
                                     self.year, self.month, self.day)
                if isinstance(self, datetime.datetime):
                    result = self
                else:
                    result = datetime.datetime(year=self.year, month=self.month, day=self.day)

                return result + delta

        return NotImplemented

    def __mul__(self, other):
        if isinstance(self, Interval):
            return Interval(self.months * other,
                            self.days * other,
                            self.time * other)
        elif isinstance(other, Interval):
            return Interval(other.months * self,
                            other.days * self,
                            other.time * self)

        return NotImplemented

    def __nonzero__(self):
        return (self.months != 0
                or self.days != 0
                or self.time != 0)


cdef interval_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    buf.write_int32(16)
    buf.write_int64(obj.time)
    buf.write_int32(obj.days)
    buf.write_int32(obj.months)


cdef interval_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int32_t days
        int32_t months
        int64_t time

    time = hton.unpack_int64(buf.read(8))
    days = hton.unpack_int32(buf.read(4))
    months = hton.unpack_int32(buf.read(4))

    return Interval(months, days, time)


cdef init_datetime_codecs():
    register_core_codec(DATEOID,
                        <encode_func>&date_encode,
                        <decode_func>&date_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(TIMEOID,
                        <encode_func>&time_encode,
                        <decode_func>&time_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(TIMETZOID,
                        <encode_func>&timetz_encode,
                        <decode_func>&timetz_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(TIMESTAMPOID,
                        <encode_func>&timestamp_encode,
                        <decode_func>&timestamp_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(TIMESTAMPTZOID,
                        <encode_func>&timestamptz_encode,
                        <decode_func>&timestamptz_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(INTERVALOID,
                        <encode_func>&interval_encode,
                        <decode_func>&interval_decode,
                        PG_FORMAT_BINARY)

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
