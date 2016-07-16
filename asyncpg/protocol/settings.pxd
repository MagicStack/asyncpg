# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef class ConnectionSettings:
    cdef:
        str _encoding
        object _codec
        dict _settings
        bint _is_utf8
        DataCodecConfig _data_codecs

    cdef add_setting(self, str name, str val)
    cdef inline is_encoding_utf8(self)
    cpdef inline get_text_codec(self)
    cpdef inline register_data_types(self, types)
    cpdef inline add_python_codec(
        self, typeoid, typename, typeschema, typekind, encoder,
        decoder, binary)
    cpdef inline set_builtin_type_codec(
        self, typeoid, typename, typeschema, typekind, alias_to)
    cpdef inline Codec get_data_codec(self, uint32_t oid)
