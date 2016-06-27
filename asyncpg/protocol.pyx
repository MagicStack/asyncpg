# cython: language_level=3

DEF DEBUG = 1

cimport cython
cimport cpython

import asyncio
import codecs
import collections
import socket

from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t

from .python cimport PyMem_Malloc, PyMem_Realloc, PyMem_Calloc, PyMem_Free, \
                     PyMemoryView_GET_BUFFER, PyMemoryView_Check, \
                     PyUnicode_AsUTF8AndSize
from cpython cimport PyBuffer_FillInfo, PyBytes_AsString

from . import exceptions
from . import encodings
from . import types as apg_types

from . cimport hton


include "consts.pxi"
include "pgtypes.pxi"

include "settings.pyx"
include "buffer.pyx"

include "codecs/base.pyx"
include "codecs/text.pyx"
include "codecs/bytea.pyx"
include "codecs/json.pyx"
include "codecs/datetime.pyx"
include "codecs/float.pyx"
include "codecs/int.pyx"
include "codecs/numeric.pyx"
include "codecs/uuid.pyx"
include "codecs/array.pyx"
include "codecs/record.pyx"
include "codecs/init.pyx"

include "coreproto.pyx"
include "prepared_stmt.pyx"


cdef dict TYPE_CODECS_CACHE = {}


cdef class BaseProtocol(CoreProtocol):

    def __init__(self, address, connect_waiter, user, password, dbname, loop):
        CoreProtocol.__init__(self, user, password, dbname)
        self._loop = loop
        self._address = address
        self._hash = (self._address, self._dbname)

        self._connect_waiter = connect_waiter
        self._waiter = None
        self._state = STATE_NOT_CONNECTED
        self._N = 0

        try:
            self._type_codecs_cache = TYPE_CODECS_CACHE[self._hash]
        except KeyError:
            self._type_codecs_cache = TYPE_CODECS_CACHE[self._hash] = {}

        self._prepared_stmt = None

        self._id = 0

    def _add_types(self, types):
        cdef:
            Codec elem_codec
            list comp_elem_codecs

        for ti in types:
            oid = ti['oid']

            if self._get_codec(oid) is not None:
                continue

            name = ti['name']
            schema = ti['ns']
            array_element_oid = ti['elemtype']
            comp_type_attrs = ti['attrtypoids']
            base_type = ti['basetype']

            if name.startswith('_') and array_element_oid:
                name = '{}[]'.format(name[1:])

            if array_element_oid:
                # Array type
                elem_codec = self._get_codec(array_element_oid)
                if elem_codec is None:
                    raise RuntimeError(
                        'no codec for array element type {}'.format(
                            array_element_oid))
                self._type_codecs_cache[oid] = \
                    Codec.new_array_codec(oid, name, schema, elem_codec)

            elif comp_type_attrs:
                # Composite element
                comp_elem_codecs = []

                for typoid in comp_type_attrs:
                    elem_codec = self._get_codec(typoid)
                    if elem_codec is None:
                        raise RuntimeError(
                            'no codec for composite attribute type {}'.format(
                                typoid))
                    comp_elem_codecs.append(elem_codec)

                self._type_codecs_cache[oid] = \
                    Codec.new_composite_codec(
                        oid, name, schema, comp_elem_codecs,
                        comp_type_attrs,
                        {name: i for i, name in enumerate(ti['attrnames'])})

            elif ti['kind'] == b'd' and base_type:
                elem_codec = self._get_codec(base_type)
                if elem_codec is None:
                    raise RuntimeError(
                        'no codec for array element type {}'.format(
                            base_type))

                self._type_codecs_cache[oid] = elem_codec
            else:
                raise NotImplementedError

    def _add_python_codec(self, typeoid, typename, typeschema, typekind,
                          encoder, decoder, binary):
        if self._get_codec(typeoid) is not None:
            raise ValueError('cannot override codec for type {}'.format(
                typeoid))

        format = PG_FORMAT_BINARY if binary else PG_FORMAT_TEXT

        self._type_codecs_cache[typeoid] = \
            Codec.new_python_codec(typeoid, typename, typeschema, typekind,
                                   encoder, decoder, format)

    def clear_type_cache(self):
        self._type_codecs_cache.clear()

    def get_settings(self):
        return self._settings

    def query(self, query):
        self._start_state(STATE_QUERY)
        self._waiter = self._create_future()
        self._query(query)
        return self._waiter

    def prepare(self, name, query):
        self._N = 0
        self._start_state(STATE_PREPARE_BIND)
        if name is None:
            name = self._gen_id('prepared_statement')
        if self._prepared_stmt is not None:
            raise RuntimeError('another prepared statement is set')

        self._prepared_stmt = PreparedStatementState(name, self)

        self._waiter = self._create_future()
        self._parse(name, query)
        return self._waiter

    def execute(self, state, args):
        if type(state) is not PreparedStatementState:
            raise TypeError(
                'state must be an instance of PreparedStatementState')

        self._start_state(STATE_EXECUTE)
        self._prepared_stmt = <PreparedStatementState>state

        self._bind(
            "",
            state.name,
            self._prepared_stmt._encode_bind_msg(args))

        self._waiter = self._create_future()
        return self._waiter

    cdef inline Codec _get_codec(self, uint32_t oid):
        cdef Codec codec

        codec = get_core_codec(oid)
        if codec is not None:
            return codec

        try:
            return self._type_codecs_cache[oid]
        except KeyError:
            return None

    cdef inline _create_future(self):
        try:
            create_future = self._loop.create_future
        except AttributeError:
            return asyncio.Future(loop=self._loop)
        else:
            return create_future()

    cdef _gen_id(self, prefix):
        self._id += 1
        return '_{}_{}'.format(self._id, prefix)

    cdef _start_state(self, ProtocolState state):
        if self._state != STATE_READY:
            raise RuntimeError('"ready" state expected')
        if self._waiter is not None:
            raise RuntimeError('waiter is set in "ready" state')
        self._state = state

    cdef _on_result(self, Result result):
        cdef:
            ProtocolState old_state = self._state
            PreparedStatementState stmt
            object waiter

        waiter = self._waiter

        if self._state == STATE_NOT_CONNECTED:
            if self._connect_waiter is None:
                raise RuntimeError(
                    'received connection result without connect_waiter set')
            waiter = self._connect_waiter
            self._connect_waiter = None

        if waiter is None:
            raise RuntimeError(
                'received result without a Future wating for it')

        if waiter.cancelled():
            # discard the result
            self._state = STATE_READY
            self._waiter = None
            return

        if result.status == PGRES_FATAL_ERROR:
            self._prepared_stmt = None
            msg = '\n'.join(['{}: {}'.format(k, v)
                for k, v in result.err_fields.items()])
            exc_cls = exceptions.ErrorMeta.get_error_for_code(
                result.err_fields.get('C'))
            exc = exc_cls(msg)
            waiter.set_exception(exc)
            self._state = STATE_READY
            self._waiter = None
            return

        if self._state == STATE_QUERY:
            waiter.set_result(1)
            self._state = STATE_READY

        elif self._state == STATE_PREPARE_BIND:
            self._state = STATE_PREPARE_DESCRIBE
            self._describe(self._prepared_stmt.name, 0)

        elif self._state == STATE_PREPARE_DESCRIBE:
            self._N += 1
            stmt = self._prepared_stmt

            if result.parameters_desc is not None:
                stmt._set_args_desc(result.parameters_desc)

            if result.row_desc is not None:
                stmt._set_row_desc(result.row_desc)

            if (self._N == 2):
                self._prepared_stmt = None
                self._state = STATE_READY
                waiter.set_result(stmt)

            else:
                # We keep the same state.
                return

        elif self._state == STATE_EXECUTE:
            stmt = self._prepared_stmt
            self._prepared_stmt = None

            if result.rows is None:
                waiter.set_result(None)
            else:
                waiter.set_result(stmt._decode_rows(result.rows))

            self._state = STATE_READY

        elif self._state == STATE_NOT_CONNECTED:
            self._state = STATE_READY
            waiter.set_result(None)

        else:
            raise RuntimeError(
                'unknown state {} in on_result'.format(self._state))

        if self._state == old_state:
            raise RuntimeError('state was not updated in on_result')

        if self._state == STATE_READY:
            self._waiter = None


class Protocol(BaseProtocol, asyncio.Protocol):
    pass
