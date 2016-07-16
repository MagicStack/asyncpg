# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef inline record_encode_frame(ConnectionSettings settings, WriteBuffer buf,
                                WriteBuffer elem_data, uint32_t elem_count):
    buf.write_int32(4 + elem_data.len())
    # attribute count
    buf.write_int32(elem_count)
    # encoded attribute data
    buf.write_buffer(elem_data)


cdef anonymous_record_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        tuple result
        uint32_t elem_count
        int32_t elem_len
        uint32_t elem_typ
        uint32_t i
        Codec elem_codec
        FastReadBuffer elem_buf = FastReadBuffer.new()

    elem_count = hton.unpack_int32(buf.read(4))
    result = cpython.PyTuple_New(elem_count)

    for i in range(elem_count):
        elem_typ = hton.unpack_int32(buf.read(4))
        elem_len = hton.unpack_int32(buf.read(4))

        if elem_len == -1:
            elem = None
        else:
            elem_codec = settings.get_data_codec(elem_typ)
            if elem_codec is None or not elem_codec.has_decoder():
                raise RuntimeError(
                    'no decoder for type OID {}'.format(elem_typ))
            elem = elem_codec.decode(settings,
                                     elem_buf.slice_from(buf, elem_len))

        cpython.Py_INCREF(elem)
        cpython.PyTuple_SET_ITEM(result, i, elem)

    return result


cdef init_record_codecs():
    register_core_codec(RECORDOID,
                        <encode_func>NULL,
                        <decode_func>&anonymous_record_decode,
                        PG_FORMAT_BINARY)

init_record_codecs()
