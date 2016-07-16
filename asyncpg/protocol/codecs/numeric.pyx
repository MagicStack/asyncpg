# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


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
