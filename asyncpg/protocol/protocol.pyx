# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


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
                     PyUnicode_AsUTF8AndSize, PyByteArray_AsString,
                     PyByteArray_Check)

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
        self.cancel_waiter = None
        self.cancel_sent_waiter = None

        self.address = addr
        self.settings = ConnectionSettings(
            (self.address, con_args.get('database')))

        self.uid_counter = 0
        self.statement = None
        self.return_extra = False

        self.last_query = None

        self.closing = False

        self.timeout_handle = None
        self.timeout_callback = self._on_timeout
        self.completed_callback = self._on_waiter_completed

        self.queries_count = 0

        try:
            self.create_future = loop.create_future
        except AttributeError:
            self.create_future = self._create_future_fallback

    def set_connection(self, connection):
        self.connection = connection

    def get_server_pid(self):
        return self.backend_pid

    def get_settings(self):
        return self.settings

    async def prepare(self, stmt_name, query, timeout):
        if self.cancel_waiter is not None:
            await self.cancel_waiter
        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None

        self._ensure_clear_state()

        if stmt_name is None:
            self.uid_counter += 1
            stmt_name = 'stmt_{}'.format(self.uid_counter)

        self._prepare(stmt_name, query)
        self.last_query = query
        self.statement = PreparedStatementState(stmt_name, query, self)

        return await self._new_waiter(timeout)

    async def bind_execute(self, PreparedStatementState state, args,
                           str portal_name, int limit, return_extra,
                           timeout):

        if self.cancel_waiter is not None:
            await self.cancel_waiter
        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None

        self._ensure_clear_state()

        self._bind_execute(
            portal_name,
            state.name,
            state._encode_bind_msg(args),
            limit)

        self.last_query = state.query
        self.statement = state
        self.return_extra = return_extra
        self.queries_count += 1

        return await self._new_waiter(timeout)

    async def bind_execute_many(self, PreparedStatementState state, args,
                                str portal_name, timeout):

        if self.cancel_waiter is not None:
            await self.cancel_waiter
        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None

        self._ensure_clear_state()

        # Make sure the argument sequence is encoded lazily with
        # this generator expression to keep the memory pressure under
        # control.
        data_gen = (state._encode_bind_msg(b) for b in args)
        arg_bufs = iter(data_gen)

        waiter = self._new_waiter(timeout)

        self._bind_execute_many(
            portal_name,
            state.name,
            arg_bufs)

        self.last_query = state.query
        self.statement = state
        self.return_extra = False
        self.queries_count += 1

        return await waiter

    async def bind(self, PreparedStatementState state, args,
                   str portal_name, timeout):

        if self.cancel_waiter is not None:
            await self.cancel_waiter
        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None

        self._ensure_clear_state()

        self._bind(
            portal_name,
            state.name,
            state._encode_bind_msg(args))

        self.last_query = state.query
        self.statement = state

        return await self._new_waiter(timeout)

    async def execute(self, PreparedStatementState state,
                      str portal_name, int limit, return_extra,
                      timeout):

        if self.cancel_waiter is not None:
            await self.cancel_waiter
        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None

        self._ensure_clear_state()

        self._execute(
            portal_name,
            limit)

        self.last_query = state.query
        self.statement = state
        self.return_extra = return_extra
        self.queries_count += 1

        return await self._new_waiter(timeout)

    async def query(self, query, timeout):
        if self.cancel_waiter is not None:
            await self.cancel_waiter
        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None

        self._ensure_clear_state()

        self._simple_query(query)
        self.last_query = query
        self.queries_count += 1

        return await self._new_waiter(timeout)

    async def close_statement(self, PreparedStatementState state, timeout):
        if self.cancel_waiter is not None:
            await self.cancel_waiter
        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None

        self._ensure_clear_state()

        if state.refs != 0:
            raise RuntimeError(
                'cannot close prepared statement; refs == {} != 0'.format(
                    state.refs))

        self._close(state.name, False)
        state.closed = True
        return await self._new_waiter(timeout)

    def is_closed(self):
        return self.closing

    def is_connected(self):
        return not self.closing and self.con_status == CONNECTION_OK

    def abort(self):
        if self.closing:
            return
        self.closing = True
        self._handle_waiter_on_connection_lost(None)
        self._terminate()
        self.transport.abort()

    async def close(self):
        if self.cancel_waiter is not None:
            await self.cancel_waiter
        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None

        self._handle_waiter_on_connection_lost(None)
        assert self.waiter is None

        if self.closing:
            return

        self._terminate()
        self.waiter = self.create_future()
        self.closing = True
        self.transport.abort()
        return await self.waiter

    def _request_cancel(self):
        self.cancel_waiter = self.create_future()
        self.cancel_sent_waiter = self.create_future()
        self.connection._cancel_current_command(self.cancel_sent_waiter)

    def _on_timeout(self, fut):
        if self.waiter is not fut or fut.done() or \
                self.cancel_waiter is not None or \
                self.timeout_handle is None:
            return
        self._request_cancel()
        self.waiter.set_exception(asyncio.TimeoutError())

    def _on_waiter_completed(self, fut):
        if fut is not self.waiter or self.cancel_waiter is not None:
            return
        if fut.cancelled():
            if self.timeout_handle:
                self.timeout_handle.cancel()
                self.timeout_handle = None
            self._request_cancel()

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

    cdef _ensure_clear_state(self):
        if self.cancel_waiter is not None:
            raise apg_exc.InterfaceError(
                'cannot perform operation: another operation is cancelling')
        if self.closing:
            raise apg_exc.InterfaceError(
                'cannot perform operation: connection is closed')
        if self.waiter is not None or self.timeout_handle is not None:
            raise apg_exc.InterfaceError(
                'cannot perform operation: another operation is in progress')

    cdef _new_waiter(self, timeout):
        self.waiter = self.create_future()
        if timeout is not False:
            timeout = timeout or self.connection._command_timeout
            if timeout is not None and timeout > 0:
                self.timeout_handle = self.connection._loop.call_later(
                    timeout, self.timeout_callback, self.waiter)
        self.waiter.add_done_callback(self.completed_callback)
        return self.waiter

    cdef _on_result__connect(self, object waiter):
        waiter.set_result(True)

    cdef _on_result__prepare(self, object waiter):
        if ASYNCPG_DEBUG:
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
        if ASYNCPG_DEBUG:
            if self.statement is None:
                raise RuntimeError(
                    '_decode_row: statement is None')

        return self.statement._decode_row(buf, buf_len)

    cdef _dispatch_result(self):
        waiter = self.waiter
        self.waiter = None

        if ASYNCPG_DEBUG:
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

            elif self.state == PROTOCOL_BIND_EXECUTE_MANY:
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
        if self.timeout_handle is not None:
            self.timeout_handle.cancel()
            self.timeout_handle = None

        if self.cancel_waiter is not None:
            if self.waiter is None or not self.waiter.cancelled():
                self.cancel_waiter.set_result(
                    RuntimeError('invalid state after cancellation'))
            else:
                self.cancel_waiter.set_result(None)
            self.cancel_waiter = None
            self.waiter = None
            return

        try:
            self._dispatch_result()
        finally:
            self.statement = None
            self.last_query = None
            self.return_extra = False

    cdef _on_notification(self, pid, channel, payload):
        self.connection._notify(pid, channel, payload)

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

    if mapping is None:
        desc = record.ApgRecordDesc_New({}, ())
    else:
        desc = record.ApgRecordDesc_New(
            mapping, tuple(mapping) if mapping else ())

    rec = record.ApgRecord_New(desc, len(elems))
    for i in range(len(elems)):
        elem = elems[i]
        cpython.Py_INCREF(elem)
        record.ApgRecord_SET_ITEM(rec, i, elem)
    return rec


record.ApgRecord_InitTypes()
