# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import uuid


_UUID = uuid.UUID


cdef uuid_encode(ConnectionSettings settings, WriteBuffer wbuf, obj):
    if cpython.PyUnicode_Check(obj):
        obj = _UUID(obj)

    bytea_encode(settings, wbuf, obj.bytes)


cdef uuid_decode(ConnectionSettings settings, FastReadBuffer buf):
    return _UUID(bytes=bytea_decode(settings, buf))


cdef init_uuid_codecs():
    register_core_codec(UUIDOID,
                        <encode_func>&uuid_encode,
                        <decode_func>&uuid_decode,
                        PG_FORMAT_BINARY)

init_uuid_codecs()
