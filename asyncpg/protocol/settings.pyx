# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


@cython.final
cdef class ConnectionSettings:

    def __cinit__(self, conn_key):
        self._encoding = 'utf-8'
        self._is_utf8 = True
        self._settings = {}
        self._codec = codecs.lookup('utf-8')
        self._data_codecs = DataCodecConfig(conn_key)

    cdef add_setting(self, str name, str val):
        self._settings[name] = val
        if name == 'client_encoding':
            py_enc = get_python_encoding(val)
            self._codec = codecs.lookup(py_enc)
            self._encoding = self._codec.name
            self._is_utf8 = self._encoding == 'utf-8'

    cdef inline is_encoding_utf8(self):
        return self._is_utf8

    cpdef inline get_text_codec(self):
        return self._codec

    cpdef inline register_data_types(self, types):
        self._data_codecs.add_types(types)

    cpdef inline add_python_codec(self, typeoid, typename, typeschema,
                                  typekind, encoder, decoder, binary):
        self._data_codecs.add_python_codec(typeoid, typename, typeschema,
                                           typekind, encoder, decoder, binary)

    cpdef inline set_builtin_type_codec(self, typeoid, typename, typeschema,
                                        typekind, alias_to):
        self._data_codecs.set_builtin_type_codec(typeoid, typename, typeschema,
                                          typekind, alias_to)

    cpdef inline Codec get_data_codec(self, uint32_t oid):
        return self._data_codecs.get_codec(oid)

    def __getattr__(self, name):
        if not name.startswith('_'):
            try:
                return self._settings[name]
            except KeyError:
                raise AttributeError(name) from None

        return object.__getattr__(self, name)
