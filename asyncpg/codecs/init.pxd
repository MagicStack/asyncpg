cdef struct core_codec:
    object (*encode)(ConnectionSettings settings, WriteBuffer buf, obj)
    object (*decode)(ConnectionSettings settings, const char *data,
                     int32_t len)
    int16_t format


cdef enum CodecInfoType:
    CODEC_C = 0
    CODEC_PY_BYTES = 1
    CODEC_PY_TEXT = 2


cdef class CodecInfo:
    cdef:
        CodecInfoType   type
        core_codec      c_codec

        object          bytes_encode
        object          bytes_decode
        object          text_encode
        object          text_decode
