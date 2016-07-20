# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef void_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    # Void is zero bytes
    buf.write_int32(0)


cdef void_decode(ConnectionSettings settings, FastReadBuffer buf):
    # Do nothing; void will be passed as NULL so this function
    # will never be called.
    pass


cdef init_pseudo_codecs():
    # Void type is returned by SELECT void_returning_function()
    register_core_codec(VOIDOID,
                        <encode_func>&void_encode,
                        <decode_func>&void_decode,
                        PG_FORMAT_BINARY)

    # Unknown type, always decoded as text
    register_core_codec(UNKNOWNOID,
                        <encode_func>&text_encode,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)

    # OID and friends
    oid_types = [
        OIDOID, TIDOID, XIDOID, CIDOID
    ]

    for oid_type in oid_types:
        register_core_codec(oid_type,
                            <encode_func>&int4_encode,
                            <decode_func>&int4_decode,
                            PG_FORMAT_BINARY)

    # reg* types -- these are really system catalog OIDs, but
    # allow the catalog object name as an input.  We could just
    # decode these as OIDs, but handling them as text seems more
    # useful.
    #
    reg_types = [
        REGPROCOID, REGPROCEDUREOID, REGOPEROID, REGOPERATOROID,
        REGCLASSOID, REGTYPEOID, REGCONFIGOID, REGDICTIONARYOID,
        REGNAMESPACEOID, REGROLEOID, REFCURSOROID
    ]

    for reg_type in reg_types:
        register_core_codec(reg_type,
                            <encode_func>&text_encode,
                            <decode_func>&text_decode,
                            PG_FORMAT_TEXT)

    # cstring type is used by Postgres' I/O functions
    register_core_codec(CSTRINGOID,
                        <encode_func>&text_encode,
                        <decode_func>&text_decode,
                        PG_FORMAT_BINARY)

    # various system pseudotypes with no I/O
    no_io_types = [
        ANYOID, TRIGGEROID, EVENT_TRIGGEROID, LANGUAGE_HANDLEROID,
        FDW_HANDLEROID, TSM_HANDLEROID, INTERNALOID, OPAQUEOID,
        ANYELEMENTOID, ANYNONARRAYOID, PG_DDL_COMMANDOID,
    ]

    register_core_codec(ANYENUMOID,
                        NULL,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)

    for no_io_type in no_io_types:
        register_core_codec(no_io_type,
                            NULL,
                            NULL,
                            PG_FORMAT_BINARY)

    # ACL specification string
    register_core_codec(ACLITEMOID,
                        <encode_func>&text_encode,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)

    # Postgres' serialized expression tree type
    register_core_codec(PG_NODE_TREEOID,
                        NULL,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)

    # pg_lsn type -- a pointer to a location in the XLOG.
    register_core_codec(PG_LSNOID,
                        <encode_func>&int8_encode,
                        <decode_func>&int8_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(SMGROID,
                        <encode_func>&text_encode,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)


init_pseudo_codecs()
