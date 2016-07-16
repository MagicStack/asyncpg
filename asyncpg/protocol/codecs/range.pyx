# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from asyncpg import types as apg_types

# defined in postgresql/src/include/utils/rangetypes.h
DEF RANGE_EMPTY  = 0x01  # range is empty
DEF RANGE_LB_INC = 0x02  # lower bound is inclusive
DEF RANGE_UB_INC = 0x04  # upper bound is inclusive
DEF RANGE_LB_INF = 0x08  # lower bound is -infinity
DEF RANGE_UB_INF = 0x10  # upper bound is +infinity


cdef enum _RangeArgumentType:
    _RANGE_ARGUMENT_INVALID = 0
    _RANGE_ARGUMENT_TUPLE = 1
    _RANGE_ARGUMENT_RANGE = 2


cdef inline bint _range_has_lbound(flags):
    return not (flags & (RANGE_EMPTY | RANGE_LB_INF))


cdef inline bint _range_has_ubound(flags):
    return not (flags & (RANGE_EMPTY | RANGE_UB_INF))


cdef inline _RangeArgumentType _range_type(object obj):
    if cpython.PyTuple_Check(obj) or cpython.PyList_Check(obj):
        return _RANGE_ARGUMENT_TUPLE
    elif isinstance(obj, apg_types.Range):
        return _RANGE_ARGUMENT_RANGE
    else:
        return _RANGE_ARGUMENT_INVALID


cdef range_encode(ConnectionSettings settings, WriteBuffer buf,
                  object obj, uint32_t elem_oid,
                  encode_func_ex encoder, const void *encoder_arg):
    cdef:
        ssize_t obj_len
        uint8_t flags = 0
        object lower = None
        object upper = None
        WriteBuffer bounds_data = WriteBuffer.new()
        _RangeArgumentType arg_type = _range_type(obj)

    if arg_type == _RANGE_ARGUMENT_INVALID:
        raise TypeError(
            'list, tuple or Range object expected (got type {})'.format(
                type(obj)))

    elif arg_type == _RANGE_ARGUMENT_TUPLE:
        obj_len = len(obj)
        if obj_len == 2:
            lower = obj[0]
            upper = obj[1]

            if lower is None:
                flags |= RANGE_LB_INF

            if upper is None:
                flags |= RANGE_UB_INF

            flags |= RANGE_LB_INC | RANGE_UB_INC

        elif obj_len == 1:
            lower = obj[0]
            flags |= RANGE_LB_INC | RANGE_UB_INF

        elif obj_len == 0:
            flags |= RANGE_EMPTY

        else:
            raise ValueError(
                'expected 0, 1 or 2 elements in range (got {})'.format(
                    obj_len))

    else:
        if obj.isempty:
            flags |= RANGE_EMPTY
        else:
            lower = obj.lower
            upper = obj.upper

            if obj.lower_inc:
                flags |= RANGE_LB_INC
            elif lower is None:
                flags |= RANGE_LB_INF

            if obj.upper_inc:
                flags |= RANGE_UB_INC
            elif upper is None:
                flags |= RANGE_UB_INF

    if _range_has_lbound(flags):
        encoder(settings, bounds_data, lower, encoder_arg)

    if _range_has_ubound(flags):
        encoder(settings, bounds_data, upper, encoder_arg)

    buf.write_int32(1 + bounds_data.len())
    buf.write_byte(flags)
    buf.write_buffer(bounds_data)


cdef range_decode(ConnectionSettings settings, FastReadBuffer buf,
                  decode_func_ex decoder, const void *decoder_arg):
    cdef:
        uint8_t flags = <uint8_t>buf.read(1)[0]
        int32_t bound_len
        object lower = None
        object upper = None
        FastReadBuffer bound_buf = FastReadBuffer.new()

    if _range_has_lbound(flags):
        bound_len = hton.unpack_int32(buf.read(4))
        if bound_len == -1:
            lower = None
        else:
            bound_buf.slice_from(buf, bound_len)
            lower = decoder(settings, bound_buf, decoder_arg)

    if _range_has_ubound(flags):
        bound_len = hton.unpack_int32(buf.read(4))
        if bound_len == -1:
            upper = None
        else:
            bound_buf.slice_from(buf, bound_len)
            upper = decoder(settings, bound_buf, decoder_arg)

    return apg_types.Range(lower=lower, upper=upper,
                           lower_inc=(flags & RANGE_LB_INC) != 0,
                           upper_inc=(flags & RANGE_UB_INC) != 0,
                           empty=(flags & RANGE_EMPTY) != 0)


cdef init_range_codecs():
    register_core_codec(ANYRANGEOID,
                        NULL,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)


init_range_codecs()
