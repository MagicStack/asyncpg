# cython: language_level=3

DEF DEBUG = 1

cimport cython
cimport cpython

import asyncio
import codecs
import collections
import socket

from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t

from asyncpg.protocol.python cimport (
                     PyMem_Malloc, PyMem_Realloc, PyMem_Calloc, PyMem_Free,
                     PyMemoryView_GET_BUFFER, PyMemoryView_Check,
                     PyUnicode_AsUTF8AndSize, PyByteArray_AsString)

from cpython cimport PyBuffer_FillInfo, PyBytes_AsString

from asyncpg.exceptions import _base as apg_exc_base
from asyncpg import types as apg_types
from asyncpg import exceptions as apg_exc

from asyncpg.protocol cimport hton


include "consts.pxi"
include "pgtypes.pxi"

include "encodings.pyx"
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
include "codecs/hstore.pyx"
include "codecs/init.pyx"
include "codecs/special.pyx"

include "coreproto.pyx"
include "prepared_stmt.pyx"


cdef class BaseProtocol(CoreProtocol):

    def __init__(self, address, connect_waiter, user, password, database, loop):
        CoreProtocol.__init__(self, user, password, database)
        self._loop = loop
        self._address = address
        self._hash = (self._address, self._database)
        self._settings = ConnectionSettings(self._hash)
        self._last_query = None
        self._connect_waiter = connect_waiter
        self._waiter = None
        self._state = STATE_NOT_CONNECTED
        self._N = 0

        self._prepared_stmt = None

        self._id = 0

    def get_settings(self):
        return self._settings

    def query(self, query):
        self._start_state(STATE_QUERY)
        self._waiter = self._create_future()
        self._last_query = query
        self._query(query)
        return self._waiter

    def prepare(self, name, query):
        self._N = 0
        self._start_state(STATE_PREPARE_DESCRIBE)
        if name is None:
            name = self._gen_id('prepared_statement')
        if self._prepared_stmt is not None:
            raise RuntimeError('another prepared statement is set')

        self._prepared_stmt = PreparedStatementState(name, query, self)

        self._waiter = self._create_future()
        self._prepare(name, query)
        return self._waiter

    def execute(self, state, args):
        if type(state) is not PreparedStatementState:
            raise TypeError(
                'state must be an instance of PreparedStatementState')

        self._start_state(STATE_EXECUTE)
        self._prepared_stmt = <PreparedStatementState>state

        self._last_query = self._prepared_stmt.query

        self._bind(
            "",
            state.name,
            self._prepared_stmt._encode_bind_msg(args))

        self._waiter = self._create_future()
        return self._waiter

    def is_closed(self):
        return self._state == STATE_CLOSING or self._state == STATE_CLOSED

    def close_statement(self, state):
        if type(state) is not PreparedStatementState:
            raise TypeError(
                'state must be an instance of PreparedStatementState')

        self._start_state(STATE_EXECUTE)

        self._close((<PreparedStatementState>state).name, False)

        self._waiter = self._create_future()
        return self._waiter

    def close(self):
        if self._state != STATE_READY:
            # Some operation is in progress; throw an error in any
            # awaiting waiter.
            self._handle_waiter_on_connection_lost(None)
            self._state = STATE_READY

        self._start_state(STATE_CLOSING)
        self._waiter = self._create_future()
        return self._waiter

    cdef _handle_waiter_on_connection_lost(self, cause):
        if self._waiter is not None and not self._waiter.done():
            exc = apg_exc.ConnectionDoesNotExistError(
                'connection was closed in the middle of '
                'operation')
            if cause is not None:
                exc.__cause__ = cause
            self._waiter.set_exception(exc)
            self._waiter = None

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
            raise RuntimeError(
                'cannot set state {}; "ready" state expected'.format(state))
        if self._waiter is not None:
            raise RuntimeError('waiter is set in "ready" state')
        self._state = state

    cdef _set_server_parameter(self, key, val):
        self._settings.add_setting(key, val)

    cdef _decode_row(self, const char* buf, int32_t buf_len):
        return self._prepared_stmt._decode_row(buf, buf_len)

    cdef _on_result(self, Result result):
        cdef:
            ProtocolState old_state = self._state
            PreparedStatementState stmt
            object waiter

        waiter = self._waiter

        if self._state == STATE_CLOSING or self._state == STATE_CLOSED:
            # The connection is lost; if any waiter is awaiting,
            # throw an error in it
            self._handle_waiter_on_connection_lost(None)
            return

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
            exc = apg_exc_base.PostgresMessage.new(result.err_fields,
                                                   query=self._last_query)
            waiter.set_exception(exc)
            self._state = STATE_READY
            self._waiter = None
            return

        if self._state == STATE_QUERY:
            waiter.set_result(1)
            self._state = STATE_READY

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
                waiter.set_result(result.rows)

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

    cdef _on_connection_lost(self, exc):
        cdef ProtocolState last_state = self._state
        self._state = STATE_CLOSED

        if last_state is STATE_CLOSING:
            # The connection was lost because
            # Protocol.close() was called
            if exc is None:
                self._waiter.set_result(None)
            else:
                self._waiter.set_exception(exc)

        else:
            # The connection was lost because it was
            # terminated or due to another error;
            # Throw an error in any awaiting waiter.
            self._handle_waiter_on_connection_lost(exc)


class Protocol(BaseProtocol, asyncio.Protocol):
    pass
