# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


ctypedef object (*encode_func)(ConnectionSettings settings,
                               WriteBuffer buf,
                               object obj)

ctypedef object (*decode_func)(ConnectionSettings settings,
                               FastReadBuffer buf)

ctypedef object (*codec_encode_func)(Codec codec,
                                     ConnectionSettings settings,
                                     WriteBuffer buf,
                                     object obj)

ctypedef object (*codec_decode_func)(Codec codec,
                                     ConnectionSettings settings,
                                     FastReadBuffer buf)


cdef enum CodecType:
    CODEC_UNDEFINED = 0
    CODEC_C         = 1
    CODEC_PY        = 2
    CODEC_ARRAY     = 3
    CODEC_COMPOSITE = 4
    CODEC_RANGE     = 5


cdef enum CodecFormat:
    PG_FORMAT_TEXT = 0
    PG_FORMAT_BINARY = 1


cdef class Codec:
    cdef:
        uint32_t        oid

        str             name
        str             schema
        str             kind

        CodecType       type
        CodecFormat     format

        encode_func     c_encoder
        decode_func     c_decoder

        object          py_encoder
        object          py_decoder

        # arrays
        Codec           element_codec

        # composite types
        tuple           element_type_oids
        object          element_names
        list            element_codecs

        # Pointers to actual encoder/decoder functions for this codec
        codec_encode_func encoder
        codec_decode_func decoder

    cdef init(self, str name, str schema, str kind,
              CodecType type, CodecFormat format,
              encode_func c_encoder, decode_func c_decoder,
              object py_encoder, object py_decoder,
              Codec element_codec, tuple element_type_oids,
              object element_names, list element_codecs)

    cdef encode_scalar(self, ConnectionSettings settings, WriteBuffer buf,
                       object obj)

    cdef encode_array(self, ConnectionSettings settings, WriteBuffer buf,
                      object obj)

    cdef encode_range(self, ConnectionSettings settings, WriteBuffer buf,
                      object obj)

    cdef encode_composite(self, ConnectionSettings settings, WriteBuffer buf,
                          object obj)

    cdef encode_in_python(self, ConnectionSettings settings, WriteBuffer buf,
                          object obj)

    cdef decode_scalar(self, ConnectionSettings settings, FastReadBuffer buf)

    cdef decode_array(self, ConnectionSettings settings, FastReadBuffer buf)

    cdef decode_range(self, ConnectionSettings settings, FastReadBuffer buf)

    cdef decode_composite(self, ConnectionSettings settings,
                          FastReadBuffer buf)

    cdef decode_in_python(self, ConnectionSettings settings,
                          FastReadBuffer buf)

    cdef inline encode(self,
                       ConnectionSettings settings,
                       WriteBuffer buf,
                       object obj)

    cdef inline decode(self,
                       ConnectionSettings settings,
                       FastReadBuffer buf)

    cdef has_encoder(self)
    cdef has_decoder(self)
    cdef is_binary(self)

    cdef inline Codec copy(self)

    @staticmethod
    cdef Codec new_array_codec(uint32_t oid,
                               str name,
                               str schema,
                               Codec element_codec)

    @staticmethod
    cdef Codec new_range_codec(uint32_t oid,
                               str name,
                               str schema,
                               Codec element_codec)

    @staticmethod
    cdef Codec new_composite_codec(uint32_t oid,
                                   str name,
                                   str schema,
                                   list element_codecs,
                                   tuple element_type_oids,
                                   object element_names)

    @staticmethod
    cdef Codec new_python_codec(uint32_t oid,
                                str name,
                                str schema,
                                str kind,
                                object encoder,
                                object decoder,
                                CodecFormat format)


cdef class DataCodecConfig:
    cdef:
        dict _type_codecs_cache
        dict _local_type_codecs

    cdef inline Codec get_codec(self, uint32_t oid)
