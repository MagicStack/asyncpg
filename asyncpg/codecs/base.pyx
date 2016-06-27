cdef void* codec_map[MAXBUILTINOID]


cdef class Codec:

    def __cinit__(self, uint32_t oid):
        self.oid = oid
        self.type = CODEC_UNDEFINED
        self.c_encoder = self.c_decoder = NULL
        self.py_encoder = self.py_decoder = None
        self.element_codec = None

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
                raise NotImplementedError
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
            raise NotImplementedError

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
                raise NotImplementedError

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
            raise NotImplementedError

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
    cdef Codec new_array_codec(uint32_t oid, Codec element_codec):
        cdef Codec codec
        codec = Codec(oid)
        codec.element_codec = element_codec
        codec.type = CODEC_ARRAY
        codec.format = PG_FORMAT_BINARY
        return codec

    @staticmethod
    cdef Codec new_composite_codec(uint32_t oid,
                                   list element_codecs,
                                   list element_type_oids,
                                   dict element_names):
        cdef Codec codec
        codec = Codec(oid)
        codec.element_names = element_names
        codec.element_type_oids = element_type_oids
        codec.element_codecs = element_codecs
        codec.type = CODEC_COMPOSITE
        codec.format = PG_FORMAT_BINARY
        return codec

    @staticmethod
    cdef Codec new_python_codec(uint32_t oid,
                                object encoder,
                                object decoder,
                                CodecFormat format):
        cdef Codec codec
        codec = Codec(oid)
        codec.type = CODEC_PY
        codec.format = format
        codec.py_encoder = encoder
        codec.py_decoder = decoder
        return codec


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

    cdef Codec codec = Codec(oid)
    cpython.Py_INCREF(codec)  # immortalize
    codec.type = CODEC_C
    codec.format = format
    codec.c_encoder = encode
    codec.c_decoder = decode
    codec_map[oid] = <void*>codec
