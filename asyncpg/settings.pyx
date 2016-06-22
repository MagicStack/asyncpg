cdef class ConnectionSettings:
    cdef:
        str _encoding
        object _codec
        dict _settings
        bint _is_utf8

    def __cinit__(self):
        self._encoding = 'utf-8'
        self._is_utf8 = True
        self._settings = {}
        self._codec = codecs.lookup('utf-8')

    cdef add_setting(self, str name, str val):
        self._settings[name] = val
        if name == 'client_encoding':
            py_enc = encodings.get_python_encoding(val)
            self._codec = codecs.lookup(py_enc)
            self._encoding = self._codec.name
            self._is_utf8 = self._encoding == 'utf-8'

    cdef inline is_encoding_utf8(self):
        return self._is_utf8

    cdef get_codec(self):
        return self._codec

    def __getattr__(self, name):
        if not name.startswith('_'):
            try:
                return self._settings[name]
            except KeyError:
                raise AttributeError(name) from None

        return object.__getattr__(self, name)
