import decimal

_Dec = decimal.Decimal


cdef numeric_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    text_encode(settings, buf, str(obj))


cdef numeric_decode(ConnectionSettings settings, const char* data, int32_t len):
    str = text_decode(settings, data, len)
    return _Dec(str)


cdef inline void init_numeric_codecs():
    codec_map[NUMERICOID].encode = numeric_encode
    codec_map[NUMERICOID].decode = numeric_decode
    codec_map[NUMERICOID].format = PG_FORMAT_TEXT
