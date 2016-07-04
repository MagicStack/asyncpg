cdef void* codec_map[MAXBUILTINOID]
cdef dict TYPE_CODECS_CACHE = {}
cdef dict EXTRA_CODECS = {}


cdef class Codec:

    def __cinit__(self, uint32_t oid, str name, str schema, str kind):
        self.oid = oid
        self.name = name
        self.schema = schema
        self.kind = kind
        self.type = CODEC_UNDEFINED
        self.c_encoder = self.c_decoder = NULL
        self.py_encoder = self.py_decoder = None
        self.element_codec = None

    cdef inline Codec copy(self):
        cdef Codec codec

        codec = Codec(self.oid, self.name, self.schema, self.kind)
        codec.type = self.type
        codec.format = self.format
        codec.c_encoder = self.c_encoder
        codec.c_decoder = self.c_decoder
        codec.py_encoder = self.py_encoder
        codec.py_decoder = self.py_decoder
        codec.element_codec = self.element_codec

        return codec

    cdef inline encode(self,
                       ConnectionSettings settings,
                       WriteBuffer buf,
                       object obj):

        cdef:
            encode_func ef
            WriteBuffer elem_data
            int32_t i

        if self.type == CODEC_C:
            ef = self.c_encoder
            if ef is NULL:
                raise NotImplementedError(
                    'no encoder for type {}'.format(self.oid))
            ef(settings, buf, obj)

        elif self.type == CODEC_ARRAY:
            elem_data = WriteBuffer.new()
            for item in obj:
                if item is None:
                    elem_data.write_int32(-1)
                else:
                    self.element_codec.encode(settings, elem_data, item)
            array_encode_frame(settings, buf, self.element_codec.oid,
                               elem_data, len(obj))

        elif self.type == CODEC_COMPOSITE:
            elem_data = WriteBuffer.new()
            i = 0
            for item in obj:
                elem_data.write_int32(self.element_type_ids[i])
                if item is None:
                    elem_data.write_int32(-1)
                else:
                    self.element_codecs[i].encode(settings, elem_data, item)
            record_encode_frame(settings, buf, elem_data, len(obj))

        elif self.type == CODEC_PY:
            bb = self.py_encoder(obj)
            if self.format == PG_FORMAT_BINARY:
                bytea_encode(settings, buf, bb)
            else:
                text_encode(settings, buf, bb)

        else:
            raise NotImplementedError(
                'no encoder for type {}'.format(self.oid))

    cdef inline decode(self,
                       ConnectionSettings settings,
                       const char *data,
                       int32_t len):

        cdef:
            decode_func df

            # For arrays:
            list result
            int32_t ndims
            uint32_t elem_count
            const char *ptr
            uint32_t i
            int32_t elem_len
            uint32_t elem_typ
            uint32_t received_elem_typ
            Codec elem_codec

        if self.type == CODEC_C:
            df = self.c_decoder
            if df is NULL:
                raise NotImplementedError(
                    'no decoder for type {}'.format(self.oid))

            return df(settings, data, len)

        elif self.type == CODEC_ARRAY:
            result = []
            ndims = hton.unpack_int32(data)
            elem_count = hton.unpack_int32(&data[12])
            ptr = &data[20]
            if ndims > 0:
                for i in range(elem_count):
                    elem_len = hton.unpack_int32(ptr)
                    ptr += 4
                    if elem_len == -1:
                        result.append(None)
                    else:
                        result.append(self.element_codec.decode(
                            settings, ptr, elem_len))
                        ptr += elem_len
            return result

        elif self.type == CODEC_COMPOSITE:
            elem_count = hton.unpack_int32(data)
            result = []
            ptr = &data[4]
            for i in range(elem_count):
                elem_typ = self.element_type_oids[i]
                received_elem_typ = hton.unpack_int32(ptr)

                if received_elem_typ != elem_typ:
                    raise RuntimeError(
                        'unexpected attribute data type: {}, expected {}'
                            .format(received_elem_typ, elem_typ))

                ptr += 4

                elem_len = hton.unpack_int32(ptr)

                ptr += 4

                if elem_len == -1:
                    result.append(None)
                else:
                    elem_codec = self.element_codecs[i]
                    result.append(elem_codec.decode(settings, ptr, elem_len))
                    ptr += elem_len

            return Record.new(self.element_names, result)

        elif self.type == CODEC_PY:
            if self.format == PG_FORMAT_BINARY:
                bb = bytea_decode(settings, data, len)
            else:
                bb = text_decode(settings, data, len)

            return self.py_decoder(bb)

        else:
            raise NotImplementedError(
                'no decoder for type {}'.format(self.oid))

    cdef has_encoder(self):
        cdef Codec elem_codec

        if self.c_encoder is not NULL or self.py_encoder is not None:
            return True

        elif self.type == CODEC_ARRAY:
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

        elif self.type == CODEC_ARRAY:
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
        codec = Codec(oid, name, schema, 'array')
        codec.element_codec = element_codec
        codec.type = CODEC_ARRAY
        codec.format = PG_FORMAT_BINARY
        return codec

    @staticmethod
    cdef Codec new_composite_codec(uint32_t oid,
                                   str name,
                                   str schema,
                                   list element_codecs,
                                   list element_type_oids,
                                   dict element_names):
        cdef Codec codec
        codec = Codec(oid, name, schema, 'composite')
        codec.element_names = element_names
        codec.element_type_oids = element_type_oids
        codec.element_codecs = element_codecs
        codec.type = CODEC_COMPOSITE
        codec.format = PG_FORMAT_BINARY
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
        codec = Codec(oid, name, schema, kind)
        codec.type = CODEC_PY
        codec.format = format
        codec.py_encoder = encoder
        codec.py_decoder = decoder
        return codec


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
            comp_type_attrs = ti['attrtypoids']
            base_type = ti['basetype']

            if name.startswith('_') and array_element_oid:
                name = '{}[]'.format(name[1:])

            if array_element_oid:
                # Array type
                elem_codec = self.get_codec(array_element_oid)
                if elem_codec is None:
                    raise RuntimeError(
                        'no codec for array element type {}'.format(
                            array_element_oid))
                self._type_codecs_cache[oid] = \
                    Codec.new_array_codec(oid, name, schema, elem_codec)

            elif comp_type_attrs:
                # Composite element
                comp_elem_codecs = []

                for typoid in comp_type_attrs:
                    elem_codec = self.get_codec(typoid)
                    if elem_codec is None:
                        raise RuntimeError(
                            'no codec for composite attribute type {}'.format(
                                typoid))
                    comp_elem_codecs.append(elem_codec)

                self._type_codecs_cache[oid] = \
                    Codec.new_composite_codec(
                        oid, name, schema, comp_elem_codecs,
                        comp_type_attrs,
                        {name: i for i, name in enumerate(ti['attrnames'])})

            elif ti['kind'] == b'd' and base_type:
                elem_codec = self.get_codec(base_type)
                if elem_codec is None:
                    raise RuntimeError(
                        'no codec for array element type {}'.format(
                            base_type))

                self._type_codecs_cache[oid] = elem_codec
            else:
                raise NotImplementedError(
                    'unhandled data type {!r}'.format(ti))

    def add_python_codec(self, typeoid, typename, typeschema, typekind,
                         encoder, decoder, binary):
        if self.get_codec(typeoid) is not None:
            raise ValueError('cannot override codec for type {}'.format(
                typeoid))

        format = PG_FORMAT_BINARY if binary else PG_FORMAT_TEXT

        self._local_type_codecs[typeoid] = \
            Codec.new_python_codec(typeoid, typename, typeschema, typekind,
                                   encoder, decoder, format)

    def add_codec_alias(self, typeoid, typename, typeschema, typekind,
                        alias_to):
        cdef:
            Codec codec
            Codec extra_codec

        if self.get_codec(typeoid) is not None:
            raise ValueError('cannot override codec for type {}'.format(
                typeoid))

        extra_codec = get_extra_codec(alias_to)
        if extra_codec is None:
            raise ValueError('unknown alias target: {}'.format(alias_to))

        codec = extra_codec.copy()
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
    if oid >= MAXBUILTINOID:
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

    if oid >= MAXBUILTINOID:
        raise RuntimeError(
            'cannot register core codec for OID {}: it is greater '
            'than MAXBUILTINOID'.format(oid))

    cdef:
        Codec codec
        str name
        str kind

    name = TYPEMAP[oid]
    kind = 'array' if oid in TYPE_IS_ARRAY else 'scalar'

    codec = Codec(oid, name, 'pg_catalog', kind)
    cpython.Py_INCREF(codec)  # immortalize

    codec.type = CODEC_C
    codec.format = format
    codec.c_encoder = encode
    codec.c_decoder = decode
    codec_map[oid] = <void*>codec


cdef register_extra_codec(str name,
                          encode_func encode,
                          decode_func decode,
                          CodecFormat format):
    cdef:
        Codec codec
        str kind

    kind = 'scalar'

    codec = Codec(INVALIDOID, name, None, kind)
    cpython.Py_INCREF(codec)  # immortalize

    codec.type = CODEC_C
    codec.format = format
    codec.c_encoder = encode
    codec.c_decoder = decode
    EXTRA_CODECS[name] = codec


cdef inline Codec get_extra_codec(str name):
    return EXTRA_CODECS.get(name)
