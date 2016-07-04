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
