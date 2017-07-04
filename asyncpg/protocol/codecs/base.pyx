# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef void* binary_codec_map[MAXSUPPORTEDOID + 1]
cdef void* text_codec_map[MAXSUPPORTEDOID + 1]
cdef dict TYPE_CODECS_CACHE = {}
cdef dict EXTRA_CODECS = {}


@cython.final
cdef class Codec:

    def __cinit__(self, uint32_t oid):
        self.oid = oid
        self.type = CODEC_UNDEFINED

    cdef init(self, str name, str schema, str kind,
              CodecType type, CodecFormat format,
              encode_func c_encoder, decode_func c_decoder,
              object py_encoder, object py_decoder,
              Codec element_codec, tuple element_type_oids,
              object element_names, list element_codecs,
              Py_UCS4 element_delimiter):

        self.name = name
        self.schema = schema
        self.kind = kind
        self.type = type
        self.format = format
        self.c_encoder = c_encoder
        self.c_decoder = c_decoder
        self.py_encoder = py_encoder
        self.py_decoder = py_decoder
        self.element_codec = element_codec
        self.element_type_oids = element_type_oids
        self.element_codecs = element_codecs
        self.element_delimiter = element_delimiter

        if element_names is not None:
            self.element_names = record.ApgRecordDesc_New(
                element_names, tuple(element_names))
        else:
            self.element_names = None

        if type == CODEC_C:
            self.encoder = <codec_encode_func>&self.encode_scalar
            self.decoder = <codec_decode_func>&self.decode_scalar
        elif type == CODEC_ARRAY:
            if format == PG_FORMAT_BINARY:
                self.encoder = <codec_encode_func>&self.encode_array
                self.decoder = <codec_decode_func>&self.decode_array
            else:
                self.encoder = <codec_encode_func>&self.encode_array_text
                self.decoder = <codec_decode_func>&self.decode_array_text
        elif type == CODEC_RANGE:
            if format != PG_FORMAT_BINARY:
                raise RuntimeError(
                    'cannot encode type "{}"."{}": text encoding of '
                    'range types is not supported'.format(schema, name))
            self.encoder = <codec_encode_func>&self.encode_range
            self.decoder = <codec_decode_func>&self.decode_range
        elif type == CODEC_COMPOSITE:
            if format != PG_FORMAT_BINARY:
                raise RuntimeError(
                    'cannot encode type "{}"."{}": text encoding of '
                    'composite types is not supported'.format(schema, name))
            self.encoder = <codec_encode_func>&self.encode_composite
            self.decoder = <codec_decode_func>&self.decode_composite
        elif type == CODEC_PY:
            self.encoder = <codec_encode_func>&self.encode_in_python
            self.decoder = <codec_decode_func>&self.decode_in_python
        else:
            raise RuntimeError('unexpected codec type: {}'.format(type))

    cdef Codec copy(self):
        cdef Codec codec

        codec = Codec(self.oid)
        codec.init(self.name, self.schema, self.kind,
                   self.type, self.format,
                   self.c_encoder, self.c_decoder,
                   self.py_encoder, self.py_decoder,
                   self.element_codec,
                   self.element_type_oids, self.element_names,
                   self.element_codecs, self.element_delimiter)

        return codec

    cdef encode_scalar(self, ConnectionSettings settings, WriteBuffer buf,
                       object obj):
        self.c_encoder(settings, buf, obj)

    cdef encode_array(self, ConnectionSettings settings, WriteBuffer buf,
                      object obj):
        array_encode(settings, buf, obj, self.element_codec.oid,
                     codec_encode_func_ex,
                     <void*>(<cpython.PyObject>self.element_codec))

    cdef encode_array_text(self, ConnectionSettings settings, WriteBuffer buf,
                           object obj):
        return textarray_encode(settings, buf, obj,
                                codec_encode_func_ex,
                                <void*>(<cpython.PyObject>self.element_codec),
                                self.element_delimiter)

    cdef encode_range(self, ConnectionSettings settings, WriteBuffer buf,
                      object obj):
        range_encode(settings, buf, obj, self.element_codec.oid,
                     codec_encode_func_ex,
                     <void*>(<cpython.PyObject>self.element_codec))

    cdef encode_composite(self, ConnectionSettings settings, WriteBuffer buf,
                          object obj):
        cdef:
            WriteBuffer elem_data
            int i
            list elem_codecs = self.element_codecs
            ssize_t count

        count = len(obj)
        if count > _MAXINT32:
            raise ValueError('too many elements in composite type record')

        elem_data = WriteBuffer.new()
        i = 0
        for item in obj:
            elem_data.write_int32(<int32_t>self.element_type_oids[i])
            if item is None:
                elem_data.write_int32(-1)
            else:
                (<Codec>elem_codecs[i]).encode(settings, elem_data, item)
            i += 1

        record_encode_frame(settings, buf, elem_data, <int32_t>count)

    cdef encode_in_python(self, ConnectionSettings settings, WriteBuffer buf,
                          object obj):
        bb = self.py_encoder(obj)
        if self.format == PG_FORMAT_BINARY:
            bytea_encode(settings, buf, bb)
        else:
            text_encode(settings, buf, bb)

    cdef encode(self, ConnectionSettings settings, WriteBuffer buf,
                object obj):
        return self.encoder(self, settings, buf, obj)

    cdef decode_scalar(self, ConnectionSettings settings, FastReadBuffer buf):
        return self.c_decoder(settings, buf)

    cdef decode_array(self, ConnectionSettings settings, FastReadBuffer buf):
        return array_decode(settings, buf, codec_decode_func_ex,
                            <void*>(<cpython.PyObject>self.element_codec))

    cdef decode_array_text(self, ConnectionSettings settings,
                           FastReadBuffer buf):
        return textarray_decode(settings, buf, codec_decode_func_ex,
                                <void*>(<cpython.PyObject>self.element_codec),
                                self.element_delimiter)

    cdef decode_range(self, ConnectionSettings settings, FastReadBuffer buf):
        return range_decode(settings, buf, codec_decode_func_ex,
                            <void*>(<cpython.PyObject>self.element_codec))

    cdef decode_composite(self, ConnectionSettings settings,
                          FastReadBuffer buf):
        cdef:
            object result
            uint32_t elem_count
            uint32_t i
            int32_t elem_len
            uint32_t elem_typ
            uint32_t received_elem_typ
            Codec elem_codec
            FastReadBuffer elem_buf = FastReadBuffer.new()

        elem_count = <uint32_t>hton.unpack_int32(buf.read(4))
        result = record.ApgRecord_New(self.element_names, elem_count)
        for i in range(elem_count):
            elem_typ = self.element_type_oids[i]
            received_elem_typ = <uint32_t>hton.unpack_int32(buf.read(4))

            if received_elem_typ != elem_typ:
                raise RuntimeError(
                    'unexpected data type of composite type attribute {}: '
                    '{!r}, expected {!r}'
                        .format(
                            i,
                            TYPEMAP.get(received_elem_typ, received_elem_typ),
                            TYPEMAP.get(elem_typ, elem_typ)
                        )
                )

            elem_len = hton.unpack_int32(buf.read(4))
            if elem_len == -1:
                elem = None
            else:
                elem_codec = self.element_codecs[i]
                elem = elem_codec.decode(settings,
                                         elem_buf.slice_from(buf, elem_len))

            cpython.Py_INCREF(elem)
            record.ApgRecord_SET_ITEM(result, i, elem)

        return result

    cdef decode_in_python(self, ConnectionSettings settings,
                          FastReadBuffer buf):
        if self.format == PG_FORMAT_BINARY:
            bb = bytea_decode(settings, buf)
        else:
            bb = text_decode(settings, buf)

        return self.py_decoder(bb)

    cdef inline decode(self, ConnectionSettings settings, FastReadBuffer buf):
        return self.decoder(self, settings, buf)

    cdef inline has_encoder(self):
        cdef Codec elem_codec

        if self.c_encoder is not NULL or self.py_encoder is not None:
            return True

        elif self.type == CODEC_ARRAY or self.type == CODEC_RANGE:
            return self.element_codec.has_encoder()

        elif self.type == CODEC_COMPOSITE:
            for elem_codec in self.element_codecs:
                if not elem_codec.has_encoder():
                    return False
            return True

        else:
            return False

    cdef has_decoder(self):
        cdef Codec elem_codec

        if self.c_decoder is not NULL or self.py_decoder is not None:
            return True

        elif self.type == CODEC_ARRAY or self.type == CODEC_RANGE:
            return self.element_codec.has_decoder()

        elif self.type == CODEC_COMPOSITE:
            for elem_codec in self.element_codecs:
                if not elem_codec.has_decoder():
                    return False
            return True

        else:
            return False

    cdef is_binary(self):
        return self.format == PG_FORMAT_BINARY

    def __repr__(self):
        return '<Codec oid={} elem_oid={} core={}>'.format(
            self.oid,
            'NA' if self.element_codec is None else self.element_codec.oid,
            has_core_codec(self.oid))

    @staticmethod
    cdef Codec new_array_codec(uint32_t oid,
                               str name,
                               str schema,
                               Codec element_codec,
                               Py_UCS4 element_delimiter):
        cdef Codec codec
        codec = Codec(oid)
        codec.init(name, schema, 'array', CODEC_ARRAY, element_codec.format,
                   NULL, NULL, None, None, element_codec, None, None, None,
                   element_delimiter)
        return codec

    @staticmethod
    cdef Codec new_range_codec(uint32_t oid,
                               str name,
                               str schema,
                               Codec element_codec):
        cdef Codec codec
        codec = Codec(oid)
        codec.init(name, schema, 'range', CODEC_RANGE, element_codec.format,
                   NULL, NULL, None, None, element_codec, None, None, None, 0)
        return codec

    @staticmethod
    cdef Codec new_composite_codec(uint32_t oid,
                                   str name,
                                   str schema,
                                   CodecFormat format,
                                   list element_codecs,
                                   tuple element_type_oids,
                                   object element_names):
        cdef Codec codec
        codec = Codec(oid)
        codec.init(name, schema, 'composite', CODEC_COMPOSITE,
                   format, NULL, NULL, None, None, None,
                   element_type_oids, element_names, element_codecs, 0)
        return codec

    @staticmethod
    cdef Codec new_python_codec(uint32_t oid,
                                str name,
                                str schema,
                                str kind,
                                object encoder,
                                object decoder,
                                CodecFormat format):
        cdef Codec codec
        codec = Codec(oid)
        codec.init(name, schema, kind, CODEC_PY, format, NULL, NULL,
                   encoder, decoder, None, None, None, None, 0)
        return codec


# Encode callback for arrays
cdef codec_encode_func_ex(ConnectionSettings settings, WriteBuffer buf,
                          object obj, const void *arg):
    return (<Codec>arg).encode(settings, buf, obj)


# Decode callback for arrays
cdef codec_decode_func_ex(ConnectionSettings settings, FastReadBuffer buf,
                          const void *arg):
    return (<Codec>arg).decode(settings, buf)


cdef class DataCodecConfig:
    def __init__(self, cache_key):
        try:
            self._type_codecs_cache = TYPE_CODECS_CACHE[cache_key]
        except KeyError:
            self._type_codecs_cache = TYPE_CODECS_CACHE[cache_key] = {}

        self._local_type_codecs = {}

    def add_types(self, types):
        cdef:
            Codec elem_codec
            list comp_elem_codecs
            CodecFormat format
            CodecFormat elem_format
            bint has_text_elements
            Py_UCS4 elem_delim

        for ti in types:
            oid = ti['oid']

            if not ti['has_bin_io']:
                format = PG_FORMAT_TEXT
            else:
                format = PG_FORMAT_BINARY

            has_text_elements = False

            if self.get_codec(oid, format) is not None:
                continue

            name = ti['name']
            schema = ti['ns']
            array_element_oid = ti['elemtype']
            range_subtype_oid = ti['range_subtype']
            if ti['attrtypoids']:
                comp_type_attrs = tuple(ti['attrtypoids'])
            else:
                comp_type_attrs = None
            base_type = ti['basetype']

            if array_element_oid:
                # Array type (note, there is no separate 'kind' for arrays)

                # Canonicalize type name to "elemtype[]"
                if name.startswith('_'):
                    name = name[1:]
                name = '{}[]'.format(name)

                if ti['elem_has_bin_io']:
                    elem_format = PG_FORMAT_BINARY
                else:
                    elem_format = PG_FORMAT_TEXT

                elem_codec = self.get_codec(array_element_oid, elem_format)
                if elem_codec is None:
                    elem_format = PG_FORMAT_TEXT
                    elem_codec = self.declare_fallback_codec(
                        array_element_oid, name, schema)

                elem_delim = <Py_UCS4>ti['elemdelim'][0]

                self._type_codecs_cache[oid, elem_format] = \
                    Codec.new_array_codec(
                        oid, name, schema, elem_codec, elem_delim)

            elif ti['kind'] == b'c':
                if not comp_type_attrs:
                    raise RuntimeError(
                        'type record missing field types for '
                        'composite {}'.format(oid))

                # Composite type

                comp_elem_codecs = []

                for typoid in comp_type_attrs:
                    elem_codec = self.get_codec(typoid, PG_FORMAT_BINARY)
                    if elem_codec is None:
                        elem_codec = self.get_codec(typoid, PG_FORMAT_TEXT)
                        has_text_elements = True
                    if elem_codec is None:
                        raise RuntimeError(
                            'no codec for composite attribute type {}'.format(
                                typoid))
                    comp_elem_codecs.append(elem_codec)

                element_names = collections.OrderedDict()
                for i, attrname in enumerate(ti['attrnames']):
                    element_names[attrname] = i

                if has_text_elements:
                    format = PG_FORMAT_TEXT

                self._type_codecs_cache[oid, format] = \
                    Codec.new_composite_codec(
                        oid, name, schema, format, comp_elem_codecs,
                        comp_type_attrs, element_names)

            elif ti['kind'] == b'd':
                # Domain type

                if not base_type:
                    raise RuntimeError(
                        'type record missing base type for domain {}'.format(
                            oid))

                elem_codec = self.get_codec(base_type, format)
                if elem_codec is None:
                    format = PG_FORMAT_TEXT
                    elem_codec = self.declare_fallback_codec(
                        base_type, name, schema)

                self._type_codecs_cache[oid, format] = elem_codec

            elif ti['kind'] == b'r':
                # Range type

                if not range_subtype_oid:
                    raise RuntimeError(
                        'type record missing base type for range {}'.format(
                            oid))

                if ti['elem_has_bin_io']:
                    elem_format = PG_FORMAT_BINARY
                else:
                    elem_format = PG_FORMAT_TEXT

                elem_codec = self.get_codec(range_subtype_oid, elem_format)
                if elem_codec is None:
                    elem_format = PG_FORMAT_TEXT
                    elem_codec = self.declare_fallback_codec(
                        range_subtype_oid, name, schema)

                self._type_codecs_cache[oid, elem_format] = \
                    Codec.new_range_codec(oid, name, schema, elem_codec)

            elif ti['kind'] == b'e':
                # Enum types are essentially text
                self._set_builtin_type_codec(oid, name, schema, 'scalar',
                                             TEXTOID, PG_FORMAT_ANY)
            else:
                self.declare_fallback_codec(oid, name, schema)

    def add_python_codec(self, typeoid, typename, typeschema, typekind,
                         encoder, decoder, binary):
        format = PG_FORMAT_BINARY if binary else PG_FORMAT_TEXT

        self._local_type_codecs[typeoid, format] = \
            Codec.new_python_codec(typeoid, typename, typeschema, typekind,
                                   encoder, decoder, format)

        self.clear_type_cache()

    def _set_builtin_type_codec(self, typeoid, typename, typeschema, typekind,
                                alias_to, format=PG_FORMAT_ANY):
        cdef:
            Codec codec
            Codec target_codec

        if format == PG_FORMAT_ANY:
            formats = (PG_FORMAT_BINARY, PG_FORMAT_TEXT)
        else:
            formats = (format,)

        for format in formats:
            if self.get_codec(typeoid, format) is not None:
                raise ValueError('cannot override codec for type {}'.format(
                    typeoid))

            if isinstance(alias_to, int):
                target_codec = self.get_codec(alias_to, format)
            else:
                target_codec = get_extra_codec(alias_to, format)

            if target_codec is None:
                continue

            codec = target_codec.copy()
            codec.oid = typeoid
            codec.name = typename
            codec.schema = typeschema
            codec.kind = typekind

            self._local_type_codecs[typeoid, format] = codec

        if ((typeoid, PG_FORMAT_BINARY) not in self._local_type_codecs and
                (typeoid, PG_FORMAT_TEXT) not in self._local_type_codecs):
            raise ValueError('unknown alias target: {}'.format(alias_to))

    def set_builtin_type_codec(self, typeoid, typename, typeschema, typekind,
                               alias_to, format=PG_FORMAT_ANY):
        self._set_builtin_type_codec(typeoid, typename, typeschema, typekind,
                                     alias_to, format)
        self.clear_type_cache()

    def clear_type_cache(self):
        self._type_codecs_cache.clear()

    def declare_fallback_codec(self, uint32_t oid, str name, str schema):
        cdef Codec codec

        codec = self.get_codec(oid, PG_FORMAT_TEXT)
        if codec is not None:
            return codec

        if oid <= MAXBUILTINOID:
            # This is a BKI type, for which asyncpg has no
            # defined codec.  This should only happen for newly
            # added builtin types, for which this version of
            # asyncpg is lacking support.
            #
            raise NotImplementedError(
                'unhandled standard data type {!r} (OID {})'.format(
                    name, oid))
        else:
            # This is a non-BKI type, and as such, has no
            # stable OID, so no possibility of a builtin codec.
            # In this case, fallback to text format.  Applications
            # can avoid this by specifying a codec for this type
            # using Connection.set_type_codec().
            #
            self._set_builtin_type_codec(oid, name, schema, 'scalar',
                                         TEXTOID, PG_FORMAT_TEXT)

            codec = self.get_codec(oid, PG_FORMAT_TEXT)

        return codec

    cdef inline Codec get_codec(self, uint32_t oid, CodecFormat format):
        cdef Codec codec

        codec = self.get_local_codec(oid, format)
        if codec is not None:
            if codec.format != format:
                # The codec for this OID has been overridden by
                # set_{builtin}_type_codec with a different format.
                # We must respect that and not return a core codec.
                return None
            else:
                return codec

        codec = get_core_codec(oid, format)
        if codec is not None:
            return codec
        else:
            try:
                return self._type_codecs_cache[oid, format]
            except KeyError:
                return None

    cdef inline Codec get_local_codec(
            self, uint32_t oid, CodecFormat preferred_format=PG_FORMAT_BINARY):
        cdef Codec codec

        codec = self._local_type_codecs.get((oid, preferred_format))
        if codec is None:
            if preferred_format == PG_FORMAT_BINARY:
                alt_format = PG_FORMAT_TEXT
            else:
                alt_format = PG_FORMAT_BINARY

            codec = self._local_type_codecs.get((oid, alt_format))

        return codec


cdef inline Codec get_core_codec(uint32_t oid, CodecFormat format):
    cdef void *ptr
    if oid > MAXSUPPORTEDOID:
        return None
    if format == PG_FORMAT_BINARY:
        ptr = binary_codec_map[oid]
    else:
        ptr = text_codec_map[oid]
    if ptr is NULL:
        return None
    return <Codec>ptr


cdef inline int has_core_codec(uint32_t oid):
    return binary_codec_map[oid] != NULL or text_codec_map[oid] != NULL


cdef register_core_codec(uint32_t oid,
                         encode_func encode,
                         decode_func decode,
                         CodecFormat format):

    if oid > MAXSUPPORTEDOID:
        raise RuntimeError(
            'cannot register core codec for OID {}: it is greater '
            'than MAXSUPPORTEDOID ({})'.format(oid, MAXSUPPORTEDOID))

    cdef:
        Codec codec
        str name
        str kind

    name = TYPEMAP[oid]
    kind = 'array' if oid in ARRAY_TYPES else 'scalar'

    codec = Codec(oid)
    codec.init(name, 'pg_catalog', kind, CODEC_C, format, encode,
               decode, None, None, None, None, None, None, 0)
    cpython.Py_INCREF(codec)  # immortalize

    if format == PG_FORMAT_BINARY:
        binary_codec_map[oid] = <void*>codec
    else:
        text_codec_map[oid] = <void*>codec


cdef register_extra_codec(str name,
                          encode_func encode,
                          decode_func decode,
                          CodecFormat format):
    cdef:
        Codec codec
        str kind

    kind = 'scalar'

    codec = Codec(INVALIDOID)
    codec.init(name, None, kind, CODEC_C, format, encode,
               decode, None, None, None, None, None, None, 0)
    EXTRA_CODECS[name, format] = codec


cdef inline Codec get_extra_codec(str name, CodecFormat format):
    return EXTRA_CODECS.get((name, format))
