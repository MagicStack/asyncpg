cdef void* codec_map[MAXBUILTINOID]


cdef class Codec:

    def __cinit__(self, oid):
        self.oid = oid
        self.type = CODEC_UNDEFINED
        self.c_encoder = self.c_decoder = NULL
        self.py_encoder = self.py_decoder = None

    cdef inline encode(self,
                       ConnectionSettings settings,
                       WriteBuffer buf,
                       object obj):

        cdef encode_func ef

        if self.type == CODEC_C:
            ef = self.c_encoder
            if ef is NULL:
                raise NotImplementedError
            ef(settings, buf, obj)

        else:
            raise NotImplementedError

    cdef inline decode(self,
                       ConnectionSettings settings,
                       const char *data,
                       int32_t len):

        cdef decode_func df

        if self.type == CODEC_C:
            df = self.c_decoder
            if df is NULL:
                raise NotImplementedError

            return df(settings, data, len)

        else:
            raise NotImplementedError

    cdef has_encoder(self):
        return (self.type != 0 and
            (self.c_encoder is not NULL or self.py_encoder is not None))

    cdef has_decoder(self):
        return (self.type != 0 and
            (self.c_decoder is not NULL or self.py_decoder is not None))

    cdef is_binary(self):
        return self.format == PG_FORMAT_BINARY

    def __repr__(self):
        return '<Codec oid={}>'.format(self.oid)


cdef inline Codec get_core_codec(uint32_t oid):
    cdef void *ptr
    ptr = codec_map[oid]
    if ptr is NULL:
        return None
    return <Codec>ptr


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

