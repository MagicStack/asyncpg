# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from libc.math cimport abs, log10
from libc.stdio cimport snprintf

import decimal

from asyncpg.protocol cimport python

# defined in postgresql/src/backend/utils/adt/numeric.c
DEF DEC_DIGITS = 4
DEF MAX_DSCALE = 0x3FFF
DEF NUMERIC_POS = 0x0000
DEF NUMERIC_NEG = 0x4000
DEF NUMERIC_NAN = 0xC000

_Dec = decimal.Decimal


cdef numeric_encode_text(ConnectionSettings settings, WriteBuffer buf, obj):
    text_encode(settings, buf, str(obj))


cdef numeric_decode_text(ConnectionSettings settings, FastReadBuffer buf):
    return _Dec(text_decode(settings, buf))


cdef numeric_encode_binary(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        object dec
        object dt
        int64_t exponent
        int64_t i
        int64_t j
        tuple pydigits
        int64_t num_pydigits
        int16_t pgdigit
        int64_t num_pgdigits
        int16_t dscale
        int64_t dweight
        int64_t weight
        uint16_t sign
        int64_t padding_size = 0

    if isinstance(obj, _Dec):
        dec = obj
    else:
        dec = _Dec(obj)

    dt = dec.as_tuple()
    if dt.exponent == 'F':
        raise ValueError('numeric type does not support infinite values')

    if dt.exponent == 'n' or dt.exponent == 'N':
        # NaN
        sign = NUMERIC_NAN
        num_pgdigits = 0
        weight = 0
        dscale = 0
    else:
        exponent = dt.exponent
        if exponent < 0 and -exponent > MAX_DSCALE:
            raise ValueError(
                'cannot encode Decimal value into numeric: '
                'exponent is too small')

        if dt.sign:
            sign = NUMERIC_NEG
        else:
            sign = NUMERIC_POS

        pydigits = dt.digits
        num_pydigits = len(pydigits)

        dweight = num_pydigits + exponent - 1
        if dweight >= 0:
            weight = (dweight + DEC_DIGITS) // DEC_DIGITS - 1
        else:
            weight = -((-dweight - 1) // DEC_DIGITS + 1)

        if weight > 2 ** 16 - 1:
            raise ValueError(
                    'cannot encode Decimal value into numeric: '
                    'exponent is too large')

        padding_size = \
            (weight + 1) * DEC_DIGITS - (dweight + 1)
        num_pgdigits = \
            (num_pydigits + padding_size + DEC_DIGITS - 1) // DEC_DIGITS

        if num_pgdigits > 2 ** 16 - 1:
            raise ValueError(
                    'cannot encode Decimal value into numeric: '
                    'number of digits is too large')

        # Pad decimal digits to provide room for correct Postgres
        # digit alignment in the digit computation loop.
        pydigits = (0,) * DEC_DIGITS + pydigits + (0,) * DEC_DIGITS

        if exponent < 0:
            if -exponent > MAX_DSCALE:
                raise ValueError(
                    'cannot encode Decimal value into numeric: '
                    'exponent is too small')
            dscale = <int16_t>-exponent
        else:
            dscale = 0

    buf.write_int32(2 + 2 + 2 + 2 + 2 * <uint16_t>num_pgdigits)
    buf.write_int16(<int16_t>num_pgdigits)
    buf.write_int16(<int16_t>weight)
    buf.write_int16(<int16_t>sign)
    buf.write_int16(dscale)

    j = DEC_DIGITS - padding_size

    for i in range(num_pgdigits):
        pgdigit = (pydigits[j] * 1000 + pydigits[j + 1] * 100 +
                   pydigits[j + 2] * 10 + pydigits[j + 3])
        j += DEC_DIGITS
        buf.write_int16(pgdigit)


# The decoding strategy here is to form a string representation of
# the numeric var, as it is faster than passing an iterable of digits.
# For this reason the below code is pure overhead and is ~25% slower
# than the simple text decoder above.  That said, we need the binary
# decoder to support binary COPY with numeric values.
cdef numeric_decode_binary(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        uint16_t num_pgdigits = <uint16_t>hton.unpack_int16(buf.read(2))
        int16_t weight = hton.unpack_int16(buf.read(2))
        uint16_t sign = <uint16_t>hton.unpack_int16(buf.read(2))
        uint16_t dscale = <uint16_t>hton.unpack_int16(buf.read(2))
        int16_t pgdigit0
        ssize_t i
        int16_t pgdigit
        object pydigits
        ssize_t num_pydigits
        ssize_t buf_size
        int64_t exponent
        int64_t abs_exponent
        ssize_t exponent_chars
        ssize_t front_padding = 0
        ssize_t trailing_padding = 0
        ssize_t num_fract_digits
        ssize_t dscale_left
        char smallbuf[_NUMERIC_DECODER_SMALLBUF_SIZE]
        char *charbuf
        char *bufptr
        bint buf_allocated = False

    if sign == NUMERIC_NAN:
        # Not-a-number
        return _Dec('NaN')

    if num_pgdigits == 0:
        # Zero
        return _Dec('0e-' + str(dscale))

    pgdigit0 = hton.unpack_int16(buf.read(2))
    if weight >= 0:
        if pgdigit0 < 10:
            front_padding = 3
        elif pgdigit0 < 100:
            front_padding = 2
        elif pgdigit0 < 1000:
            front_padding = 1

    # Maximum possible number of decimal digits in base 10.
    num_pydigits = num_pgdigits * DEC_DIGITS + dscale
    # Exponent.
    exponent = (weight + 1) * DEC_DIGITS - front_padding
    abs_exponent = abs(exponent)
    # Number of characters required to render absolute exponent value.
    exponent_chars = <ssize_t>log10(<double>abs_exponent) + 1

    buf_size = (
        1 +                 # sign
        1 +                 # leading zero
        1 +                 # decimal dot
        num_pydigits +      # digits
        2 +                 # exponent indicator (E-,E+)
        exponent_chars +    # exponent
        1                   # null terminator char
    )

    if buf_size > _NUMERIC_DECODER_SMALLBUF_SIZE:
        charbuf = <char *>PyMem_Malloc(<size_t>buf_size)
        buf_allocated = True
    else:
        charbuf = smallbuf

    try:
        bufptr = charbuf

        if sign == NUMERIC_NEG:
            bufptr[0] = b'-'
            bufptr += 1

        bufptr[0] = b'0'
        bufptr[1] = b'.'
        bufptr += 2

        if weight >= 0:
            bufptr = _unpack_digit_stripping_lzeros(bufptr, pgdigit0)
        else:
            bufptr = _unpack_digit(bufptr, pgdigit0)

        for i in range(1, num_pgdigits):
            pgdigit = hton.unpack_int16(buf.read(2))
            bufptr = _unpack_digit(bufptr, pgdigit)

        if dscale:
            if weight >= 0:
                num_fract_digits = num_pgdigits - weight - 1
            else:
                num_fract_digits = num_pgdigits

            # Check how much dscale is left to render (trailing zeros).
            dscale_left = dscale - num_fract_digits * DEC_DIGITS
            if dscale_left > 0:
                for i in range(dscale_left):
                    bufptr[i] = <char>b'0'

            # If display scale is _less_ than the number of rendered digits,
            # dscale_left will be negative and this will strip the excess
            # trailing zeros.
            bufptr += dscale_left

        if exponent != 0:
            bufptr[0] = b'E'
            if exponent < 0:
                bufptr[1] = b'-'
            else:
                bufptr[1] = b'+'
            bufptr += 2
            snprintf(bufptr, <size_t>exponent_chars + 1, '%d',
                     <int>abs_exponent)
            bufptr += exponent_chars

        bufptr[0] = 0

        pydigits = python.PyUnicode_FromString(charbuf)

        return _Dec(pydigits)

    finally:
        if buf_allocated:
            PyMem_Free(charbuf)


cdef inline char *_unpack_digit_stripping_lzeros(char *buf, int64_t pgdigit):
    cdef:
        int64_t d
        bint significant

    d = pgdigit // 1000
    significant = (d > 0)
    if significant:
        pgdigit -= d * 1000
        buf[0] = <char>(d + <int32_t>b'0')
        buf += 1

    d = pgdigit // 100
    significant |= (d > 0)
    if significant:
        pgdigit -= d * 100
        buf[0] = <char>(d + <int32_t>b'0')
        buf += 1

    d = pgdigit // 10
    significant |= (d > 0)
    if significant:
        pgdigit -= d * 10
        buf[0] = <char>(d + <int32_t>b'0')
        buf += 1

    buf[0] = <char>(pgdigit + <int32_t>b'0')
    buf += 1

    return buf


cdef inline char *_unpack_digit(char *buf, int64_t pgdigit):
    cdef:
        int64_t d

    d = pgdigit // 1000
    pgdigit -= d * 1000
    buf[0] = <char>(d + <int32_t>b'0')

    d = pgdigit // 100
    pgdigit -= d * 100
    buf[1] = <char>(d + <int32_t>b'0')

    d = pgdigit // 10
    pgdigit -= d * 10
    buf[2] = <char>(d + <int32_t>b'0')

    buf[3] = <char>(pgdigit + <int32_t>b'0')
    buf += 4

    return buf


cdef init_numeric_codecs():
    register_core_codec(NUMERICOID,
                        <encode_func>&numeric_encode_text,
                        <decode_func>&numeric_decode_text,
                        PG_FORMAT_TEXT)

    register_core_codec(NUMERICOID,
                        <encode_func>&numeric_encode_binary,
                        <decode_func>&numeric_decode_binary,
                        PG_FORMAT_BINARY)

init_numeric_codecs()
