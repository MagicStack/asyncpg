# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef void* codec_map[MAXSUPPORTEDOID + 1]
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
              object element_names, list element_codecs):

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

        if element_names is not None:
            self.element_names = record.ApgRecordDesc_New(
                element_names, tuple(element_names))
        else:
            self.element_names = None

        if type == CODEC_C:
            self.encoder = <codec_encode_func>&self.encode_scalar
            self.decoder = <codec_decode_func>&self.decode_scalar
        elif type == CODEC_ARRAY:
            self.encoder = <codec_encode_func>&self.encode_array
            self.decoder = <codec_decode_func>&self.decode_array
        elif type == CODEC_RANGE:
            self.encoder = <codec_encode_func>&self.encode_range
            self.decoder = <codec_decode_func>&self.decode_range
        elif type == CODEC_COMPOSITE:
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
                   self.element_codecs)

        return codec

    cdef encode_scalar(self, ConnectionSettings settings, WriteBuffer buf,
                       object obj):
        self.c_encoder(settings, buf, obj)

    cdef encode_array(self, ConnectionSettings settings, WriteBuffer buf,
                      object obj):
        array_encode(settings, buf, obj, self.element_codec.oid,
                     codec_encode_func_ex,
                     <void*>(<cpython.PyObject>self.element_codec))

    cdef encode_range(self, ConnectionSettings settings, WriteBuffer buf,
                      object obj):
        range_encode(settings, buf, obj, self.element_codec.oid,
                     codec_encode_func_ex,
                     <void*>(<cpython.PyObject>self.element_codec))

    cdef encode_composite(self, ConnectionSettings settings, WriteBuffer buf,
                          object obj):
        cdef:
            WriteBuffer elem_data
            int32_t i
            list elem_codecs = self.element_codecs

        elem_data = WriteBuffer.new()
        i = 0
        for item in obj:
            elem_data.write_int32(self.element_type_oids[i])
            if item is None:
                elem_data.write_int32(-1)
            else:
                (<Codec>elem_codecs[i]).encode(settings, elem_data, item)
            i += 1

        record_encode_frame(settings, buf, elem_data, len(obj))

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

        elem_count = hton.unpack_int32(buf.read(4))
        result = record.ApgRecord_New(self.element_names, elem_count)
        for i in range(elem_count):
            elem_typ = self.element_type_oids[i]
            received_elem_typ = hton.unpack_int32(buf.read(4))

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
                               Codec element_codec):
        cdef Codec codec
        codec = Codec(oid)
        codec.init(name, schema, 'array', CODEC_ARRAY, PG_FORMAT_BINARY,
                   NULL, NULL, None, None, element_codec, None, None, None)
        return codec

    @staticmethod
    cdef Codec new_range_codec(uint32_t oid,
                               str name,
                               str schema,
                               Codec element_codec):
        cdef Codec codec
        codec = Codec(oid)
        codec.init(name, schema, 'range', CODEC_RANGE, PG_FORMAT_BINARY,
                   NULL, NULL, None, None, element_codec, None, None, None)
        return codec

    @staticmethod
    cdef Codec new_composite_codec(uint32_t oid,
                                   str name,
                                   str schema,
                                   list element_codecs,
                                   tuple element_type_oids,
                                   object element_names):
        cdef Codec codec
        codec = Codec(oid)
        codec.init(name, schema, 'composite', CODEC_COMPOSITE,
                   PG_FORMAT_BINARY, NULL, NULL, None, None, None,
                   element_type_oids, element_names, element_codecs)
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
                   encoder, decoder, None, None, None, None)
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

        for ti in types:
            oid = ti['oid']

            if self.get_codec(oid) is not None:
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

                elem_codec = self.get_codec(array_element_oid)
                if elem_codec is None:
                    raise RuntimeError(
                        'no codec for array element type {}'.format(
                            array_element_oid))

                self._type_codecs_cache[oid] = \
                    Codec.new_array_codec(oid, name, schema, elem_codec)

            elif ti['kind'] == b'c':
                if not comp_type_attrs:
                    raise RuntimeError(
                        'type record missing field types for '
                        'composite {}'.format(oid))

                # Composite type

                comp_elem_codecs = []

                for typoid in comp_type_attrs:
                    elem_codec = self.get_codec(typoid)
                    if elem_codec is None:
                        raise RuntimeError(
                            'no codec for composite attribute type {}'.format(
                                typoid))
                    comp_elem_codecs.append(elem_codec)

                element_names = collections.OrderedDict()
                for i, attrname in enumerate(ti['attrnames']):
                    element_names[attrname] = i

                self._type_codecs_cache[oid] = \
                    Codec.new_composite_codec(
                        oid, name, schema, comp_elem_codecs,
                        comp_type_attrs,
                        element_names)

            elif ti['kind'] == b'd':
                # Domain type

                if not base_type:
                    raise RuntimeError(
                        'type record missing base type for domain {}'.format(
                            oid))

                elem_codec = self.get_codec(base_type)
                if elem_codec is None:
                    raise RuntimeError(
                        'no codec for domain base type {}'.format(base_type))

                self._type_codecs_cache[oid] = elem_codec

            elif ti['kind'] == b'r':
                # Range type

                if not range_subtype_oid:
                    raise RuntimeError(
                        'type record missing base type for range {}'.format(
                            oid))

                elem_codec = self.get_codec(range_subtype_oid)
                if elem_codec is None:
                    raise RuntimeError(
                        'no codec for range element type {}'.format(
                            range_subtype_oid))

                self._type_codecs_cache[oid] = \
                    Codec.new_range_codec(oid, name, schema, elem_codec)

            else:
                if oid <= MAXBUILTINOID:
                    # This is a non-BKI type, for which ayncpg has no
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
                    self.set_builtin_type_codec(oid, name, schema, 'scalar',
                                                UNKNOWNOID)

    def add_python_codec(self, typeoid, typename, typeschema, typekind,
                         encoder, decoder, binary):
        if self.get_codec(typeoid) is not None:
            raise ValueError('cannot override codec for type {}'.format(
                typeoid))

        format = PG_FORMAT_BINARY if binary else PG_FORMAT_TEXT

        self._local_type_codecs[typeoid] = \
            Codec.new_python_codec(typeoid, typename, typeschema, typekind,
                                   encoder, decoder, format)

    def set_builtin_type_codec(self, typeoid, typename, typeschema, typekind,
                               alias_to):
        cdef:
            Codec codec
            Codec target_codec

        if self.get_codec(typeoid) is not None:
            raise ValueError('cannot override codec for type {}'.format(
                typeoid))

        if isinstance(alias_to, int):
            target_codec = self.get_codec(alias_to)
        else:
            target_codec = get_extra_codec(alias_to)

        if target_codec is None:
            raise ValueError('unknown alias target: {}'.format(alias_to))

        codec = target_codec.copy()
        codec.oid = typeoid
        codec.name = typename
        codec.schema = typeschema
        codec.kind = typekind

        self._local_type_codecs[typeoid] = codec

    def clear_type_cache(self):
        self._type_codecs_cache.clear()

    cdef inline Codec get_codec(self, uint32_t oid):
        cdef Codec codec

        codec = get_core_codec(oid)
        if codec is not None:
            return codec

        try:
            return self._type_codecs_cache[oid]
        except KeyError:
            try:
                return self._local_type_codecs[oid]
            except KeyError:
                return None


cdef inline Codec get_core_codec(uint32_t oid):
    cdef void *ptr
    if oid > MAXSUPPORTEDOID:
        return None
    ptr = codec_map[oid]
    if ptr is NULL:
        return None
    return <Codec>ptr


cdef inline int has_core_codec(uint32_t oid):
    return codec_map[oid] != NULL


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
               decode, None, None, None, None, None, None)
    cpython.Py_INCREF(codec)  # immortalize
    codec_map[oid] = <void*>codec


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
               decode, None, None, None, None, None, None)
    EXTRA_CODECS[name] = codec


cdef inline Codec get_extra_codec(str name):
    return EXTRA_CODECS.get(name)
