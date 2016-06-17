import datetime


cdef long pg_epoch_datetime = cpython.PyLong_AsLong(
    datetime.datetime(2000, 1, 1).timestamp())

cdef long pg_epoch_datetime_utc = cpython.PyLong_AsLong(
    datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc).timestamp())

pg_epoch_date = datetime.date(2000, 1, 1)
cdef long pg_date_offset_ord = cpython.PyLong_AsLong(pg_epoch_date.toordinal())

# Binary representations of infinity for datetimes.
cdef long long pg_time_infinity = 0x7ff0000000000000
cdef long long pg_time_negative_infinity = 0xfff0000000000000
cdef long long pg_time64_infinity = 0x7fffffffffffffff
cdef long long pg_time64_negative_infinity = 0x8000000000000000
cdef long pg_date_infinity = 0x7fffffff
cdef long pg_date_negative_infinity = 0x80000000

infinity_datetime = datetime.datetime(
    datetime.MAXYEAR, 12, 31, 23, 59, 59, 999999)

cdef long infinity_datetime_ord = cpython.PyLong_AsLong(
    infinity_datetime.toordinal())

negative_infinity_datetime = datetime.datetime(
    datetime.MINYEAR, 1, 1, 0, 0, 0, 0)

cdef long negative_infinity_datetime_ord = cpython.PyLong_AsLong(
    negative_infinity_datetime.toordinal())

infinity_date = datetime.date(datetime.MAXYEAR, 12, 31)

cdef long infinity_date_ord = cpython.PyLong_AsLong(
    infinity_date.toordinal())

negative_infinity_date = datetime.date(datetime.MINYEAR, 1, 1)

cdef long negative_infinity_date_ord = cpython.PyLong_AsLong(
    negative_infinity_date.toordinal())

date_from_ordinal = datetime.date.fromordinal


cdef date_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        long ordinal = cpython.PyLong_AsLongLong(obj.toordinal())
        long pg_ordinal

    if ordinal == infinity_date_ord:
        pg_ordinal = pg_date_infinity
    elif ordinal == negative_infinity_date_ord:
        pg_ordinal = pg_date_negative_infinity
    else:
        pg_ordinal = ordinal - pg_date_offset_ord

    buf.write_int32(pg_ordinal)


cdef date_decode(ConnectionSettings settings, const char* data):
    cdef int32_t pg_ordinal = hton.unpack_int32(data)

    if pg_ordinal == pg_date_infinity:
        return infinity_date
    elif pg_ordinal == pg_date_negative_infinity:
        return negative_infinity_date
    else:
        return date_from_ordinal(pg_ordinal + pg_date_offset_ord)


cdef inline void init_datetime_codecs():
    codec_map[DATEOID].encode = date_encode
    codec_map[DATEOID].decode = date_decode
