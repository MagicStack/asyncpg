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

        if self.type == CODEC_C:
            ef = self.c_encoder
            if ef is NULL:
                raise NotImplementedError
            ef(settings, buf, obj)

        elif self.type == CODEC_ARRAY:
            elem_data = WriteBuffer.new()
            for item in obj:
                self.element_codec.encode(settings, elem_data, item)
            array_encode(settings, buf, self.element_codec.oid,
                         elem_data, len(obj))

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

        if self.type == CODEC_C:
            df = self.c_decoder
            if df is NULL:
                raise NotImplementedError

            return df(settings, data, len)

        elif self.type == CODEC_ARRAY:
            result = []
            ndims = hton.unpack_int32(data)
            elem_count = hton.unpack_int32(&data[12])
            ptr = &data[24]
            if ndims > 0:
                for i from 0 <= i < elem_count:
                    result.append(
                        self.element_codec.decode(settings, ptr, len-24))
                    ptr += 4 + self.element_size
            return result

        else:
            raise NotImplementedError

    cdef has_encoder(self):
        return (self.type != 0 and
            (self.c_encoder is not NULL or
             self.py_encoder is not None or
             (self.type == CODEC_ARRAY and self.element_codec.has_encoder())))

    cdef has_decoder(self):
        return (self.type != 0 and
            (self.c_decoder is not NULL or
             self.py_decoder is not None or
             (self.type == CODEC_ARRAY and self.element_codec.has_decoder())))

    cdef is_binary(self):
        return self.format == PG_FORMAT_BINARY

    def __repr__(self):
        return '<Codec oid={} elem_oid={} core={}>'.format(
            self.oid,
            'NA' if self.element_codec is None else self.element_codec.oid,
            has_core_codec(self.oid))

    @staticmethod
    cdef Codec new_array_codec(uint32_t oid,
                               Codec element_codec,
                               ssize_t element_size):
        cdef Codec codec
        codec = Codec(oid)
        codec.element_codec = element_codec
        codec.element_size = element_size
        codec.type = CODEC_ARRAY
        codec.format = PG_FORMAT_BINARY
        return codec


cdef inline Codec get_core_codec(uint32_t oid):
    cdef void *ptr
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
        raise RuntimeError

    cdef Codec codec = Codec(oid)
    cpython.Py_INCREF(codec)  # immortalize
    codec.type = CODEC_C
    codec.format = format
    codec.c_encoder = encode
    codec.c_decoder = decode
    codec_map[oid] = <void*>codec
