import decimal


_Dec = decimal.Decimal


cdef numeric_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    text_encode(settings, buf, str(obj))


cdef numeric_decode(ConnectionSettings settings, FastReadBuffer buf):
    return _Dec(text_decode(settings, buf))


cdef init_numeric_codecs():
    register_core_codec(NUMERICOID,
                        <encode_func>&numeric_encode,
                        <decode_func>&numeric_decode,
                        PG_FORMAT_TEXT)

init_numeric_codecs()
