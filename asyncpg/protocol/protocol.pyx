# cython: language_level=3

cimport cython
cimport cpython

import asyncio
import codecs
import collections
import socket

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t

from asyncpg.protocol cimport record

from asyncpg.protocol.python cimport (
                     PyMem_Malloc, PyMem_Realloc, PyMem_Calloc, PyMem_Free,
                     PyMemoryView_GET_BUFFER, PyMemoryView_Check,
                     PyUnicode_AsUTF8AndSize, PyByteArray_AsString)

from cpython cimport PyBuffer_FillInfo, PyBytes_AsString

from asyncpg.exceptions import _base as apg_exc_base
from asyncpg import types as apg_types
from asyncpg import exceptions as apg_exc

from asyncpg.protocol cimport hton


include "__debug.pxi"
include "consts.pxi"
include "pgtypes.pxi"

include "encodings.pyx"
include "settings.pyx"
include "buffer.pyx"

include "codecs/base.pyx"

# String types.  Need to go first, as other codecs may rely on
# text decoding/encoding.
include "codecs/bytea.pyx"
include "codecs/text.pyx"

# Builtin types, in lexicographical order.
include "codecs/bits.pyx"
include "codecs/datetime.pyx"
include "codecs/float.pyx"
include "codecs/geometry.pyx"
include "codecs/int.pyx"
include "codecs/json.pyx"
include "codecs/money.pyx"
include "codecs/network.pyx"
include "codecs/numeric.pyx"
include "codecs/tsearch.pyx"
include "codecs/txid.pyx"
include "codecs/uuid.pyx"

# Various pseudotypes and system types
include "codecs/misc.pyx"

# nonscalar
include "codecs/array.pyx"
include "codecs/range.pyx"
include "codecs/record.pyx"

# contrib
include "codecs/hstore.pyx"

include "coreproto.pyx"
include "prepared_stmt.pyx"


cdef class BaseProtocol(CoreProtocol):
    def __init__(self, addr, connected_fut, con_args, loop):
        CoreProtocol.__init__(self, con_args)

        self.loop = loop
        self.waiter = connected_fut

        self.address = addr
        self.settings = ConnectionSettings(
            (self.address, con_args.get('database')))

        self.uid_counter = 0
        self.statement = None
        self.return_extra = False

        self.last_query = None

        self.closing = False

        try:
            self.create_future = loop.create_future
        except AttributeError:
            self.create_future = self._create_future_fallback

    def get_settings(self):
        return self.settings

    def prepare(self, stmt_name, query):
        waiter = self._new_waiter()
        try:
            if stmt_name is None:
                self.uid_counter += 1
                stmt_name = 'stmt_{}'.format(self.uid_counter)
            self._prepare(stmt_name, query)
        except Exception as exc:
            waiter.set_exception(exc)
            self.waiter = None
        else:
            self.last_query = query
            self.statement = PreparedStatementState(stmt_name, query, self)
        return waiter

    def bind_execute(self, PreparedStatementState state, args,
                     str portal_name, int limit, return_extra):
        waiter = self._new_waiter()
        try:
            self._bind_execute(
                portal_name,
                state.name,
                state._encode_bind_msg(args),
                limit)
        except Exception as exc:
            waiter.set_exception(exc)
            self.waiter = None
        else:
            self.last_query = state.query
            self.statement = state
            self.return_extra = return_extra
        return waiter

    def bind(self, PreparedStatementState state, args, str portal_name):
        waiter = self._new_waiter()
        try:
            self._bind(
                portal_name,
                state.name,
                state._encode_bind_msg(args))
        except Exception as exc:
            waiter.set_exception(exc)
            self.waiter = None
        else:
            self.last_query = state.query
            self.statement = state
        return waiter

    def execute(self, PreparedStatementState state,
                str portal_name, int limit, return_extra):
        waiter = self._new_waiter()
        try:
            self._execute(
                portal_name,
                limit)
        except Exception as exc:
            waiter.set_exception(exc)
            self.waiter = None
        else:
            self.last_query = state.query
            self.statement = state
            self.return_extra = return_extra
        return waiter

    def query(self, query):
        waiter = self._new_waiter()
        try:
            self._simple_query(query)
        except Exception as exc:
            waiter.set_exception(exc)
            self.waiter = None
        else:
            self.last_query = query
        return waiter

    def close_statement(self, PreparedStatementState state):
        waiter = self._new_waiter()
        try:
            if state.refs != 0:
                raise RuntimeError(
                    'cannot close prepared statement; refs == {} != 0'.format(
                        state.refs))
            self._close(state.name, False)
        except Exception as exc:
            waiter.set_exception(exc)
            self.waiter = None
        else:
            state.closed = True
        return waiter

    def is_closed(self):
        return self.closing

    def abort(self):
        self._handle_waiter_on_connection_lost(None)
        self.transport.abort()

    def close(self):
        self._handle_waiter_on_connection_lost(None)
        assert self.waiter is None

        self.waiter = self.create_future()
        if self.closing:
            self.waiter.set_result(None)
        else:
            self.closing = True
            self.transport.abort()

        return self.waiter

    def _create_future_fallback(self):
        return asyncio.Future(loop=self.loop)

    cdef _handle_waiter_on_connection_lost(self, cause):
        if self.waiter is not None and not self.waiter.done():
            exc = apg_exc.ConnectionDoesNotExistError(
                'connection was closed in the middle of '
                'operation')
            if cause is not None:
                exc.__cause__ = cause
            self.waiter.set_exception(exc)
        self.waiter = None

    cdef _set_server_parameter(self, name, val):
        self.settings.add_setting(name, val)

    cdef _new_waiter(self):
        if self.closing:
            raise apg_exc.InterfaceError(
                'cannot perform operation: connection is closed')
        if self.waiter is not None:
            raise apg_exc.InterfaceError(
                'cannot perform operation: another operation is in progress')
        self.waiter = self.create_future()
        return self.waiter

    cdef _on_result__connect(self, object waiter):
        waiter.set_result(True)

    cdef _on_result__prepare(self, object waiter):
        IF DEBUG:
            if self.statement is None:
                raise RuntimeError(
                    '_on_result__prepare: statement is None')

        if self.result_param_desc is not None:
            self.statement._set_args_desc(self.result_param_desc)
        if self.result_row_desc is not None:
            self.statement._set_row_desc(self.result_row_desc)
        waiter.set_result(self.statement)

    cdef _on_result__bind_and_exec(self, object waiter):
        if self.return_extra:
            waiter.set_result((
                self.result,
                self.result_status_msg,
                self.result_execute_completed))
        else:
            waiter.set_result(self.result)

    cdef _on_result__bind(self, object waiter):
        waiter.set_result(self.result)

    cdef _on_result__close_stmt_or_portal(self, object waiter):
        waiter.set_result(self.result)

    cdef _on_result__simple_query(self, object waiter):
        waiter.set_result(self.result_status_msg.decode(self.encoding))

    cdef _decode_row(self, const char* buf, int32_t buf_len):
        IF DEBUG:
            if self.statement is None:
                raise RuntimeError(
                    '_decode_row: statement is None')

        return self.statement._decode_row(buf, buf_len)

    cdef _dispatch_result(self):
        waiter = self.waiter
        self.waiter = None

        IF DEBUG:
            if waiter is None:
                raise RuntimeError('_on_result: waiter is None')

        if waiter.cancelled():
            return

        if waiter.done():
            raise RuntimeError('_on_result: waiter is done')

        if self.result_type == RESULT_FAILED:
            if isinstance(self.result, dict):
                exc = apg_exc_base.PostgresMessage.new(
                    self.result, query=self.last_query)
            else:
                exc = self.result
            waiter.set_exception(exc)
            return

        try:
            if self.state == PROTOCOL_AUTH:
                self._on_result__connect(waiter)

            elif self.state == PROTOCOL_PREPARE:
                self._on_result__prepare(waiter)

            elif self.state == PROTOCOL_BIND_EXECUTE:
                self._on_result__bind_and_exec(waiter)

            elif self.state == PROTOCOL_EXECUTE:
                self._on_result__bind_and_exec(waiter)

            elif self.state == PROTOCOL_BIND:
                self._on_result__bind(waiter)

            elif self.state == PROTOCOL_CLOSE_STMT_PORTAL:
                self._on_result__close_stmt_or_portal(waiter)

            elif self.state == PROTOCOL_SIMPLE_QUERY:
                self._on_result__simple_query(waiter)

            else:
                raise RuntimeError(
                    'got result for unknown protocol state {}'.
                    format(self.state))

        except Exception as exc:
            waiter.set_exception(exc)

    cdef _on_result(self):
        try:
            self._dispatch_result()
        finally:
            self.statement = None
            self.last_query = None
            self.return_extra = False

    cdef _on_connection_lost(self, exc):
        if self.closing:
            # The connection was lost because
            # Protocol.close() was called
            if self.waiter is not None and not self.waiter.done():
                if exc is None:
                    self.waiter.set_result(None)
                else:
                    self.waiter.set_exception(exc)
            self.waiter = None
        else:
            # The connection was lost because it was
            # terminated or due to another error;
            # Throw an error in any awaiting waiter.
            self.closing = True
            self._handle_waiter_on_connection_lost(exc)


class Protocol(BaseProtocol, asyncio.Protocol):
    pass


def _create_record(object mapping, tuple elems):
    # Exposed only for testing purposes.

    cdef:
        object rec
        int32_t i

    rec = record.ApgRecord_New(mapping, len(elems))
    for i in range(len(elems)):
        elem = elems[i]
        cpython.Py_INCREF(elem)
        record.ApgRecord_SET_ITEM(rec, i, elem)
    return rec
