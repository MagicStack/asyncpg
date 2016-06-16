# cython: language_level=3

DEF DEBUG = 1

cimport cython
cimport cpython

import asyncio
import collections

from .python cimport PyMem_Malloc, PyMem_Realloc, PyMem_Calloc, PyMem_Free, \
                     PyMemoryView_GET_BUFFER, PyMemoryView_Check
from cpython cimport PyBuffer_FillInfo, PyBytes_AsString

include "buffer.pyx"
include "codecs.pyx"


DEF CON_STATE_INIT = 0
DEF CON_STATE_READY = 10
DEF CON_STATE_QUERY_IN_PROGRESS = 20
DEF CON_STATE_CLOSED = 100



cdef class BaseProtocol:
    cdef:
        object transport
        ReadBuffer buffer
        int _state

        ####### Options:

        str _user

        ####### Connection State:

        dict _statuses

        int _backend_pid
        int _backend_secret

    def __init__(self, user):
        self.buffer = ReadBuffer()
        self.transport = None

        self._user = user

        self._state = CON_STATE_INIT

        self._statuses = {}
        self._backend_pid = 0
        self._backend_secret = 0

    cdef _write(self, WriteBuffer buf):
        self.transport.write(memoryview(buf))

    cdef inline _read_server_messages(self):
        cdef:
            char mtype

        while self.buffer.has_message() and self._state != CON_STATE_CLOSED:
            mtype = self.buffer.get_message_type()
            try:
                self._dispatch_server_message(mtype)
            except Exception as ex:
                self._fatal_error(ex)
            finally:
                self.buffer.discard_message()

    cdef _dispatch_server_message(self, char mtype):
        if mtype == b'R':
            self._parse_server_authentication()
        elif mtype == b'S':
            self._parse_server_parameter_status()
        elif mtype == b'K':
            self._parse_server_backend_key_data()
        elif mtype == b'Z':
            self._parse_server_ready_for_query()
        elif mtype == b'T':
            self._parse_server_row_description()
        elif mtype == b'D':
            self._parse_server_data_row()
        elif mtype == b'C':
            self._parse_server_command_complete()
        elif mtype == b'E':
            self._parse_server_error_response()
        else:
            raise RuntimeError(
                'unsupported message type {!r}'.format(chr(mtype)))

    cdef _parse_server_authentication(self):
        cdef int status
        status = self.buffer.read_int32()
        if status == 0:
            # AuthenticationOk
            self._state = CON_STATE_READY
            self.on_authed()
        else:
            raise RuntimeError(
                'unsupported status {} for Authentication (R) '
                'message'.format(status))

    cdef _parse_server_parameter_status(self):
        key = self.buffer.read_cstr().decode()
        val = self.buffer.read_cstr().decode()
        self._statuses[key] = val

    cdef _parse_server_backend_key_data(self):
        self._backend_pid = self.buffer.read_int32()
        self._backend_secret = self.buffer.read_int32()

    cdef _parse_server_ready_for_query(self):
        cdef char byte
        byte = self.buffer.read_byte()
        if byte == b'I':
            # ReadyForQuery: I -- Idle, not in transaction block.
            # Ignore this?
            pass
        else:
            raise NotImplementedError(byte)

    cdef _parse_server_row_description(self):
        if self._state != CON_STATE_QUERY_IN_PROGRESS:
            raise RuntimeError(
                'invalid state in '
                'BaseProtocol._parse_server_row_description')

        self.on_query_row_description(self.buffer.consume_message())

    cdef _parse_server_data_row(self):
        if self._state != CON_STATE_QUERY_IN_PROGRESS:
            # TODO add other compatible states
            raise RuntimeError(
                'invalid state in '
                'BaseProtocol._parse_server_data_row')

        self.on_query_row(self.buffer.consume_message())

    cdef _parse_server_command_complete(self):
        if self._state != CON_STATE_QUERY_IN_PROGRESS:
            # TODO add other compatible states
            raise RuntimeError(
                'invalid state in '
                'BaseProtocol._parse_server_command_complete')

        # TODO: Ignore tag, or do we need it for something?
        tag = self.buffer.read_cstr()

        self.on_query_done(tag)
        self._state = CON_STATE_READY

    cdef _parse_server_error_response(self):
        cdef:
            char code
            bytes message
            dict parsed = {}

        # TODO
        self._state = CON_STATE_READY

        while True:
            code = self.buffer.read_byte()
            if code == 0:
                break

            message = self.buffer.read_cstr()

            parsed[chr(code)] = message.decode()

        self.on_error(parsed)

    cdef _open(self):
        if self._state != CON_STATE_INIT:
            raise RuntimeError('invalid state in BaseProtocol._open')

        # Assemble a startup message
        buf = WriteBuffer()

        # protocol version
        buf.write_int16(3)
        buf.write_int16(0)

        buf.write_cstr(b'client_encoding')
        buf.write_cstr(b"'utf-8'")

        if self._user:
            buf.write_cstr(b'user')
            buf.write_cstr(self._user.encode())

        buf.write_cstr(b'')

        # Send the buffer
        outbuf = WriteBuffer()
        outbuf.write_int32(buf.len() + 4)
        outbuf.write_buffer(buf)
        self._write(outbuf)

    cdef _fatal_error(self, exc):
        if self._state == CON_STATE_CLOSED:
            return

        self._state = CON_STATE_CLOSED

        if self.transport is not None:
            self.transport.close()

        self.fatal_error(exc)

    # API for subclasses:

    def query(self, query):
        if self._state != CON_STATE_READY:
            raise RuntimeError('invalid state in BaseProtocol._query')

        self._state = CON_STATE_QUERY_IN_PROGRESS

        # Compose Query message
        buf = WriteBuffer.new_message(b'Q')
        buf.write_cstr(query.encode())
        buf.end_message()
        self._write(buf)

    def on_query_row_description(self, data):
        pass

    def on_query_row(self, data):
        pass

    def on_query_done(self, tag):
        pass

    def on_authed(self):
        pass

    def fatal_error(self, exc):
        pass

    def on_error(self, lines):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)
        self._read_server_messages()

    def connection_made(self, transport):
        self.transport = transport

        try:
            self._open()
        except Exception as ex:
            self._fatal_error(ex)


class Protocol(BaseProtocol, asyncio.Protocol):

    def __init__(self, connect_waiter, user, loop):
        BaseProtocol.__init__(self, user=user)
        self._loop = loop

        self._connect_waiter = connect_waiter

        self._query_waiter = None
        self._query_rows_desc = None
        self._query_rows = []

    def _try_report_error(self, exc):
        if self._connect_waiter is not None:
            self._connect_waiter.set_exception(exc)
            self._connect_waiter = None
            return True

        if self._query_waiter is not None:
            self._query_waiter.set_exception(exc)
            self._query_waiter = None
            return True

        return False

    def query(self, query, waiter):
        self._query_waiter = waiter
        self._query_result = []
        BaseProtocol.query(self, query)

    def on_query_row_description(self, data):
        self._query_rows_desc = data

    def on_query_row(self, data):
        self._query_rows.append(data)

    def on_query_done(self, tag):
        self._query_waiter.set_result(
            (self._query_rows_desc, self._query_rows))
        self._query_waiter = None

    def on_authed(self):
        self._connect_waiter.set_result(True)
        self._connect_waiter = None

    def on_error(self, lines):
        msg = '\n'.join(['{}: {}'.format(k, v) for k, v in lines.items()])
        exc = RuntimeError(msg)
        self._try_report_error(exc)

    def fatal_error(self, exc):
        if not self._try_report_error(exc):
            # TODO
            raise exc
