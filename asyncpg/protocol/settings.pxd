cdef class ConnectionSettings:
    cdef:
        str _encoding
        object _codec
        dict _settings
        bint _is_utf8

    cdef add_setting(self, str name, str val)
    cdef inline is_encoding_utf8(self)
    cdef get_codec(self)
