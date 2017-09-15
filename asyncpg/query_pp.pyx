# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncpg
import functools

cimport cpython

from asyncpg.protocol.python cimport (
     PyUnicode_KIND, PyUnicode_READ, PyUnicode_WRITE, PyUnicode_DATA,
     PyUnicode_FromKindAndData)


cdef enum ParamLexMode:
    MODE_NORMAL
    MODE_STRING
    MODE_ESTRING
    MODE_QUOTE
    MODE_COMMENT
    MODE_BLOCK_COMMENT
    MODE_DOLLAR


cdef inline Py_ssize_t _buf_append(
        str source, Py_ssize_t source_from, Py_ssize_t source_to,
        int dest_kind, void *dest, Py_ssize_t to_dest_pos):

    cdef:
        void *source_buf = PyUnicode_DATA(source)
        int source_kind = PyUnicode_KIND(source)
        Py_ssize_t i
        Py_UCS4 ch

    for i in range(source_from, source_to):
        ch = PyUnicode_READ(source_kind, source_buf, i)
        PyUnicode_WRITE(dest_kind, dest, to_dest_pos, ch)
        to_dest_pos += 1

    return to_dest_pos


cpdef transform_kwargs(str query):
    # This optimized lexer is 10x faster on average
    # than an equivalent pure-Python implementation.

    cdef:
        ParamLexMode mode = MODE_NORMAL
        Py_ssize_t i = 0
        Py_ssize_t query_len = len(query)
        bint lexing = True
        bint eof
        dict named_params
        list named_params_list

        str arg_name
        str tag
        str marker

        int ukind

        Py_UCS4 ch
        Py_UCS4 prev_ch
        Py_UCS4 prev_prev_ch

        bint has_positional_only = False

        void *query_buf = PyUnicode_DATA(query)
        void *new_query_buf = NULL
        Py_ssize_t new_query_buf_pos = 0

        Py_ssize_t dollar_started = 0

    if not query_len:
        return query, None

    ukind = PyUnicode_KIND(query)

    new_query_buf = cpython.PyMem_Malloc(
        <size_t>((ukind * 1.4 * query_len) * sizeof(void*)));
    if new_query_buf == NULL:
        raise MemoryError

    try:
        named_params_list = []
        named_params = {}

        ch = PyUnicode_READ(ukind, query_buf, 0)
        PyUnicode_WRITE(ukind, new_query_buf, new_query_buf_pos, ch)
        new_query_buf_pos += 1

        while lexing:
            if mode == MODE_NORMAL:
                # Normal lexer mode when we are not parsing a string literal,
                # a quoted identifier, a comment, or a `$..` sequence.

                while True:
                    i += 1
                    if i >= query_len:
                        lexing = False
                        break
                    prev_ch = ch
                    ch = PyUnicode_READ(ukind, query_buf, i)

                    PyUnicode_WRITE(
                        ukind, new_query_buf, new_query_buf_pos, ch)
                    new_query_buf_pos += 1

                    if ch == <Py_UCS4>u'"':
                        mode = MODE_QUOTE
                        break
                    elif ch == <Py_UCS4>u"'":
                        prev_prev_ch = <Py_UCS4>u'\0'
                        if query_len > 2:
                            prev_prev_ch = PyUnicode_READ(
                                ukind, query_buf, i - 2)

                        if (prev_ch == <Py_UCS4>u'E' or
                                prev_ch == <Py_UCS4>u'e') and (
                                    # Check that we don't have
                                    # `CASE'\'` situation.
                                    (prev_prev_ch < <Py_UCS4>u'A' or
                                        prev_prev_ch > <Py_UCS4>u'Z') and
                                    (prev_prev_ch < <Py_UCS4>u'a' or
                                        prev_prev_ch > <Py_UCS4>u'z')):
                            mode = MODE_ESTRING
                        else:
                            mode = MODE_STRING
                        break
                    elif prev_ch == <Py_UCS4>u'-' and ch == <Py_UCS4>u'-':
                        mode = MODE_COMMENT
                        break
                    elif prev_ch == <Py_UCS4>u'/' and ch == <Py_UCS4>u'*':
                        mode = MODE_BLOCK_COMMENT
                        break
                    elif ch == <Py_UCS4>u'$':
                        mode = MODE_DOLLAR
                        break

            elif mode == MODE_QUOTE:
                # Quoted identifier, such as `"name"`.

                while True:
                    i += 1
                    if i >= query_len:
                        lexing = False
                        break
                    prev_ch = ch
                    ch = PyUnicode_READ(ukind, query_buf, i)

                    PyUnicode_WRITE(
                        ukind, new_query_buf, new_query_buf_pos, ch)
                    new_query_buf_pos += 1

                    if ch == <Py_UCS4>u'"':
                        mode = MODE_NORMAL
                        break

            elif mode == MODE_STRING:
                # Regular string literal, such as `'aaaa'`.

                while True:
                    i += 1
                    if i >= query_len:
                        lexing = False
                        break
                    prev_ch = ch
                    ch = PyUnicode_READ(ukind, query_buf, i)

                    PyUnicode_WRITE(
                        ukind, new_query_buf, new_query_buf_pos, ch)
                    new_query_buf_pos += 1

                    if ch == <Py_UCS4>u"'":
                        mode = MODE_NORMAL
                        break

            elif mode == MODE_ESTRING:
                # String literal prefixed with `e` or `E`.
                # For example: `E'aaa'` or `e'\''`.

                while True:
                    i += 1
                    if i >= query_len:
                        lexing = False
                        break
                    prev_ch = ch
                    ch = PyUnicode_READ(ukind, query_buf, i)

                    PyUnicode_WRITE(
                        ukind, new_query_buf, new_query_buf_pos, ch)
                    new_query_buf_pos += 1


                    if ch == <Py_UCS4>u"'":
                        if prev_ch == <Py_UCS4>u'\\':
                            continue
                        else:
                            mode = MODE_NORMAL
                            break

            elif mode == MODE_COMMENT:
                # Single line "--" comment.

                while True:
                    i += 1
                    if i >= query_len:
                        lexing = False
                        break
                    prev_ch = ch
                    ch = PyUnicode_READ(ukind, query_buf, i)

                    PyUnicode_WRITE(
                        ukind, new_query_buf, new_query_buf_pos, ch)
                    new_query_buf_pos += 1

                    if ch == <Py_UCS4>u"\n":
                        mode = MODE_NORMAL
                        break

            elif mode == MODE_BLOCK_COMMENT:
                # Block /* .. */ comment.

                while True:
                    i += 1
                    if i >= query_len:
                        lexing = False
                        break
                    prev_ch = ch
                    ch = PyUnicode_READ(ukind, query_buf, i)

                    PyUnicode_WRITE(
                        ukind, new_query_buf, new_query_buf_pos, ch)
                    new_query_buf_pos += 1

                    if prev_ch == <Py_UCS4>u'*' and ch == <Py_UCS4>u'/':
                        mode = MODE_NORMAL
                        break

            elif mode == MODE_DOLLAR:
                # Whenever we see '$' we switch to this mode.
                # The '$' character can be a start of:
                # - an argument, such as `$1` or `$foo`;
                # - a quoted string, such as `$$ .. $$` or `$foo$ .. $foo$`.

                while True:
                    i += 1
                    eof = False
                    if i >= query_len:
                        ch = <Py_UCS4>u'\0'
                        eof = True
                    else:
                        prev_ch = ch
                        ch = PyUnicode_READ(ukind, query_buf, i)

                    if ch == <Py_UCS4>u'$':
                        # We found a second '$' character, this looks like
                        # the beginning of a quoted string like
                        # `$$` or `$foo$`.

                        assert ch != <Py_UCS4>u'\0'

                        if dollar_started == 0:
                            tag = ''
                        else:
                            tag = query[dollar_started:i]
                        marker = '$' + tag + '$'

                        dollar_started = 0

                        new_query_buf_pos = _buf_append(
                            tag, 0, len(tag),
                            ukind, new_query_buf, new_query_buf_pos)

                        PyUnicode_WRITE(
                            ukind, new_query_buf, new_query_buf_pos, ch)
                        new_query_buf_pos += 1

                        res = query.find(marker, i)
                        if res == -1:
                            # Can't find the matching end, as in
                            # "SELECT $aa$ ... " query.  The query is
                            # likely invalid, but we don't care.
                            new_query_buf_pos = _buf_append(
                                query, i + 1, len(query),
                                ukind, new_query_buf, new_query_buf_pos)

                            lexing = False
                            break
                        else:
                            # Found the end marker.
                            new_query_buf_pos = _buf_append(
                                query, i + 1, res + len(marker),
                                ukind, new_query_buf, new_query_buf_pos)

                            i = res + len(marker) - 1
                            mode = MODE_NORMAL
                            break

                    elif not (ch >= <Py_UCS4>u'A' and ch <= <Py_UCS4>u'Z' or
                              ch >= <Py_UCS4>u'a' and ch <= <Py_UCS4>u'z' or
                              ch >= <Py_UCS4>u'0' and ch <= <Py_UCS4>u'9' or
                              ch == <Py_UCS4>u'_') or eof:

                        # This looks like an argument.

                        if dollar_started == 0:
                            if not eof:
                                PyUnicode_WRITE(
                                    ukind, new_query_buf,
                                    new_query_buf_pos, ch)
                                new_query_buf_pos += 1
                            mode = MODE_NORMAL
                            break
                        else:
                            arg_name = query[dollar_started:i]

                        dollar_started = 0
                        positional_only = arg_name.isdecimal()

                        if positional_only and not named_params:
                            has_positional_only = True

                            new_query_buf_pos = _buf_append(
                                arg_name, 0, len(arg_name),
                                ukind, new_query_buf, new_query_buf_pos)

                        elif not positional_only and not has_positional_only:
                            if arg_name[0].isdecimal():
                                raise ValueError(
                                    'invalid argument name {!r}: first '
                                    'character is a digit')

                            if arg_name not in named_params:
                                named_params[arg_name] = len(named_params) + 1
                                named_params_list.append(arg_name)

                            arg_name = str(named_params[arg_name])
                            new_query_buf_pos = _buf_append(
                                arg_name, 0, len(arg_name),
                                ukind, new_query_buf, new_query_buf_pos)

                        else:
                            raise ValueError(
                                'queries with both named and positional-only '
                                'arguments are not supported')

                        if not eof:
                            PyUnicode_WRITE(
                                ukind, new_query_buf, new_query_buf_pos, ch)
                            new_query_buf_pos += 1
                        mode = MODE_NORMAL
                        break

                    else:
                        assert ch != <Py_UCS4>u'\0'
                        if dollar_started == 0:
                            dollar_started = i

                    if i >= query_len:
                        lexing = False
                        break

        if named_params_list:
            new_query = PyUnicode_FromKindAndData(
                ukind, new_query_buf, new_query_buf_pos)
            return new_query, tuple(named_params_list)
        else:
            return query, None

    finally:
        cpython.PyMem_Free(new_query_buf)


@functools.lru_cache(250)
def keyword_parameters(query):
    return transform_kwargs(query)
