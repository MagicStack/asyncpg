ctypedef object (*encode_func)(ConnectionSettings settings,
                               WriteBuffer buf,
                               object obj)

ctypedef object (*decode_func)(ConnectionSettings settings,
                               const char *data,
                               int32_t len)


cdef enum CodecType:
    CODEC_UNDEFINED = 0
    CODEC_C         = 1
    CODEC_PY        = 2


cdef enum CodecFormat:
    PG_FORMAT_TEXT = 0
    PG_FORMAT_BINARY = 1


cdef class Codec:
    cdef:
        uint32_t        oid

        CodecType       type
        CodecFormat     format

        encode_func     c_encoder
        decode_func     c_decoder

        object          py_encoder
        object          py_decoder

    cdef inline encode(self,
                       ConnectionSettings settings,
                       WriteBuffer buf,
                       object obj)

    cdef inline decode(self,
                       ConnectionSettings settings,
                       const char *data,
                       int32_t len)

    cdef has_encoder(self)
    cdef has_decoder(self)
    cdef is_binary(self)
