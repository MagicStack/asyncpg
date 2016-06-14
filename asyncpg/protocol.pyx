# cython: language_level=3

DEF DEBUG = 1

cimport cython

import asyncio
import collections

include "buffer.pyx"

from .python cimport PyMem_Malloc, PyMem_Realloc, PyMem_Calloc, PyMem_Free
from cpython cimport PyBuffer_FillInfo, PyBytes_AsString


cdef class BaseProtocol:
    cdef:
        object transport
        ReadBuffer buffer

        ####### Connection State:
        bint _authenticated
        dict _statuses

        int _backend_pid
        int _backend_secret

    def __cinit__(self):
        self.buffer = ReadBuffer()

        self._authenticated = False
        self._statuses = {}
        self._backend_pid = 0
        self._backend_secret = 0

    cdef _write(self, WriteBuffer buf):
        self.transport.write(memoryview(buf))

    cdef inline _read_server_messages(self):
        cdef:
            char mtype

        while self.buffer.has_message():
            mtype = self.buffer.get_message_type()
            try:
                self._dispatch_server_message(mtype)
            finally:
                self.buffer.discard_message()

    cdef _dispatch_server_message(self, char mtype):
        if mtype == b'R':
            self._parse_server_authentication()
        elif mtype == b'S':
            self._parse_server_parameter_status()
        elif mtype == b'K':
            self._parse_server_backend_key_data()
        else:
            raise RuntimeError(
                'unsupported message type {!r}'.format(chr(mtype)))

    cdef _parse_server_authentication(self):
        cdef int status
        status = self.buffer.read_int32()
        if status == 0:
            # AuthenticationOk
            self._authenticated = True
        else:
            raise RuntimeError(
                'unsupported status {} for Authentication (R) message'.format(
                    status))

    cdef _parse_server_parameter_status(self):
        key = self.buffer.read_cstr().decode()
        val = self.buffer.read_cstr().decode()
        self._statuses[key] = val

    cdef _parse_server_backend_key_data(self):
        self._backend_pid = self.buffer.read_int32()
        self._backend_secret = self.buffer.read_int32()

    def data_received(self, data):
        self.buffer.feed_data(data)
        self._read_server_messages()

    def connection_made(self, transport):
        self.transport = transport

    def open(self, user='postgres'):
        # Assemble a startup message
        buf = WriteBuffer()

        # protocol version
        buf.write_int16(3)
        buf.write_int16(0)

        buf.write_cstr(b'client_encoding')
        buf.write_cstr(b"'utf-8'")

        buf.write_cstr(b'user')
        buf.write_cstr(user.encode())

        buf.write_cstr(b'')

        # Send the buffer
        outbuf = WriteBuffer()
        outbuf.write_int32(buf.len() + 4)
        outbuf.write_buffer(buf)
        self._write(outbuf)


class Protocol(BaseProtocol, asyncio.Protocol):
    pass
