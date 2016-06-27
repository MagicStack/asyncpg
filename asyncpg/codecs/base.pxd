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
    CODEC_ARRAY     = 3
    CODEC_COMPOSITE = 4


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

        # arrays
        Codec           element_codec

        # composite types
        list            element_type_oids
        dict            element_names
        list            element_codecs

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

    @staticmethod
    cdef Codec new_array_codec(uint32_t oid,
                               Codec element_codec)

    @staticmethod
    cdef Codec new_composite_codec(uint32_t oid,
                                   list element_codecs,
                                   list element_type_oids,
                                   dict element_names)

    @staticmethod
    cdef Codec new_python_codec(uint32_t oid,
                                object encoder,
                                object decoder,
                                CodecFormat format)
