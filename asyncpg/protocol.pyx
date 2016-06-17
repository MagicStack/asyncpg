# cython: language_level=3

DEF DEBUG = 1

cimport cython
cimport cpython

import asyncio
import collections

from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t

from .python cimport PyMem_Malloc, PyMem_Realloc, PyMem_Calloc, PyMem_Free, \
                     PyMemoryView_GET_BUFFER, PyMemoryView_Check
from cpython cimport PyBuffer_FillInfo, PyBytes_AsString


cdef class ConnectionSettings:
    pass


include "pgtypes.pxd"

include "buffer.pyx"

include "codecs/int.pyx"
include "codecs/float.pyx"
include "codecs/datetime.pyx"
include "codecs/init.pyx"


cdef enum ConnectionStatus:
    CONNECTION_OK = 0
    CONNECTION_BAD = 1
    CONNECTION_STARTED = 2           # Waiting for connection to be made.
    CONNECTION_MADE = 3              # Connection OK; waiting to send.
    CONNECTION_AWAITING_RESPONSE = 4 # Waiting for a response from the
                                     # postmaster.
    CONNECTION_AUTH_OK = 5           # Received authentication; waiting for
                                     # backend startup.
    CONNECTION_SETENV = 6            # Negotiating environment.
    CONNECTION_SSL_STARTUP = 7       # Negotiating SSL.
    CONNECTION_NEEDED = 8            # Internal state: connect() needed

cdef enum AsyncStatus:
    # defines the state of the query-execution state machine
    PGASYNC_IDLE = 0                 # nothing's happening, dude
    PGASYNC_BUSY = 1                 # query in progress
    PGASYNC_READY = 2                # result ready for PQgetResult
    PGASYNC_COPY_IN = 3              # Copy In data transfer in progress
    PGASYNC_COPY_OUT = 4             # Copy Out data transfer in progress
    PGASYNC_COPY_BOTH = 5            # Copy In/Out data transfer in progress

cdef enum QueryClass:
    # tracks which query protocol we are now executing
    PGQUERY_SIMPLE = 0               # simple Query protocol (PQexec)
    PGQUERY_EXTENDED = 1             # full Extended protocol (PQexecParams)
    PGQUERY_PREPARE = 2              # Parse only (PQprepare)
    PGQUERY_DESCRIBE = 3             # Describe Statement or Portal

cdef enum ExecStatusType:
    PGRES_EMPTY_QUERY = 0            # empty query string was executed
    PGRES_COMMAND_OK = 1             # a query command that doesn't return
                                     # anything was executed properly by the
                                     # backend
    PGRES_TUPLES_OK = 2              # a query command that returns tuples was
                                     # executed properly by the backend,
                                     # PGresult contains the result tuples
    PGRES_COPY_OUT = 3               # Copy Out data transfer in progress
    PGRES_COPY_IN = 4                # Copy In data transfer in progress
    PGRES_BAD_RESPONSE = 5           # an unexpected response was recv'd from
                                     # the backend
    PGRES_NONFATAL_ERROR = 6         # notice or warning message
    PGRES_FATAL_ERROR = 7            # query failed
    PGRES_COPY_BOTH = 8              # Copy In/Out data transfer in progress
    PGRES_SINGLE_TUPLE = 9           # single tuple from larger resultset

cdef enum TransactionStatus:
    PQTRANS_IDLE = 0                 # connection idle
    PQTRANS_ACTIVE = 1               # command in progress
    PQTRANS_INTRANS = 2              # idle, within transaction block
    PQTRANS_INERROR = 3              # idle, within failed transaction
    PQTRANS_UNKNOWN = 4              # cannot determine status

cdef enum MessageDispatchLoop:
    DISPATCH_CONTINUE = 0
    DISPATCH_STOP = 1


@cython.no_gc_clear
@cython.freelist(_BUFFER_FREELIST_SIZE)
cdef class Result:
    cdef:
        ExecStatusType status

        # message broken into fields
        dict err_fields
        # text of triggering query, if available
        str  err_query

        # cmd status from the query
        bytes cmd_status

        object parameters_desc
        object row_desc
        list rows

    def __cinit__(self):
        self.err_fields = self.err_query = None
        self.rows = None
        self.row_desc = None
        self.parameters_desc = None

    @staticmethod
    cdef Result new(ExecStatusType status):
        cdef Result res
        # TODO: see fe_exec.c:PQmakeEmptyPGresult
        res = Result.__new__(Result)
        res.status = status
        return res


cdef class CoreProtocol:
    cdef:
        object transport
        ReadBuffer buffer

        ####### Options:

        str _user
        str _encoding

        ####### Connection State:

        dict _settings

        int _backend_pid
        int _backend_secret

        # result being constructed
        Result            _result

        ConnectionStatus  _status
        AsyncStatus       _async_status
        QueryClass        _query_class
        TransactionStatus _xact_status

        WriteBuffer _after_sync

    def __init__(self, user):
        self.buffer = ReadBuffer()
        self.transport = None

        self._user = user

        self._encoding = 'utf-8'
        self._settings = {}
        self._backend_pid = 0
        self._backend_secret = 0

        self._result = None

        self._status = CONNECTION_BAD
        self._async_status = PGASYNC_IDLE
        self._xact_status = PQTRANS_IDLE

        self._after_sync = None

    cdef _write(self, WriteBuffer buf):
        self.transport.write(memoryview(buf))

    cdef inline _read_server_messages(self):
        cdef:
            char mtype

        while self.buffer.has_message():
            mtype = self.buffer.get_message_type()
            try:
                if self._dispatch_server_message(mtype) == DISPATCH_STOP:
                    return
            except Exception as ex:
                self._fatal_error(ex)
            finally:
                self.buffer.discard_message()

    cdef inline MessageDispatchLoop _dispatch_server_message(self, char mtype):
        # Modeled after libpq/fe-protocol3.c:pqParseInput3

        print('>>>>>', chr(mtype))

        if mtype == b'A':
            # Notify message; ignore it for now.
            self.buffer.consume_message()

        elif mtype == b'N':
            # Notice; ignore it
            self.buffer.consume_message()

        elif self._async_status != PGASYNC_BUSY:
            # If not IDLE state, just wait ...
            if self._async_status != PGASYNC_IDLE:
                return DISPATCH_STOP

            if mtype == b'E':
                # Notice; ignore it
                self.buffer.consume_message()

            elif mtype == b'S':
                self._parse_server_parameter_status()

            else:
                print("!!! message type {} arrived from server "
                      "while idle".format(chr(mtype)))
                # Discard the message
                self.buffer.consume_message()
        else:
            # In BUSY state, we can process everything.

            if mtype == b'C':
                # Command complete
                if self._result is None:
                    self._result = Result.new(PGRES_COMMAND_OK)
                self._result.cmd_status = self.buffer.read_cstr()

                self._async_status = PGASYNC_READY
                self._push_result()

            elif mtype == b'E':
                # Error return
                self._parse_server_error_response(True)
                self._async_status = PGASYNC_READY
                self._push_result()

            elif mtype == b'Z':
                # Backend is ready for new query
                self._parse_server_ready_for_query()

                if self._after_sync is not None:
                    self._async_status = PGASYNC_BUSY
                    self._write(self._after_sync)
                    self._after_sync = None

            elif mtype == b'I':
                # Empty query
                if self._result is None:
                    self._result = Result.new(PGRES_EMPTY_QUERY)
                self._async_status = PGASYNC_READY
                self._push_result()

            elif mtype == b'1':
                # Parse Complete
                # If we're doing prepare, we're done; else ignore
                if self._query_class == PGQUERY_PREPARE:
                    if self._result is None:
                        self._result = Result.new(PGRES_COMMAND_OK)
                    self._async_status = PGASYNC_READY
                    self._push_result()

            elif mtype == b'2':
                # Bind Complete
                pass

            elif mtype == b'3':
                # Close Complete
                pass

            elif mtype == b'S':
                # Parameter Status
                self._parse_server_parameter_status()

            elif mtype == b'K':
                # secret key data from the backend

                # This is expected only during backend startup, but it's
                # just as easy to handle it as part of the main loop.
                # Save the data and continue processing.
                self._parse_server_backend_key_data()

            elif mtype == b'T':
                # Row Description

                if (self._result is not None and
                        self._result.status == PGRES_FATAL_ERROR):
                    # We've already choked for some reason.  Just discard
                    # the data till we get to the end of the query.
                    self.buffer.consume_message()

                elif (self._result is None or
                        self._query_class == PGQUERY_DESCRIBE):
                    # First 'T' in a query sequence
                    self._parse_server_row_description()

                else:
                    # A new 'T' message is treated as the start of
                    # another PGresult.  (It is not clear that this is
                    # really possible with the current backend.) We stop
                    # parsing until the application accepts the current
                    # result.
                    self._async_status = PGASYNC_READY
                    self._push_result()
                    return DISPATCH_STOP

            elif mtype == b'n':
                # No Data

                # NoData indicates that we will not be seeing a
                # RowDescription message because the statement or portal
                # inquired about doesn't return rows.
                #
                # If we're doing a Describe, we have to pass something
                # back to the client, so set up a COMMAND_OK result,
                # instead of TUPLES_OK.  Otherwise we can just ignore
                # this message.
                if self._query_class == PGQUERY_DESCRIBE:
                    if self._result is None:
                        self._result = Result.new(PGRES_COMMAND_OK)
                    self._async_status = PGASYNC_READY
                    self._push_result()

            elif mtype == b't':
                # Parameter Description
                self._parse_server_parameter_description()

            elif mtype == b'D':
                # Data Row

                if (self._result is not None and
                        self._result.status == PGRES_TUPLES_OK):
                    # Read another tuple of a normal query response
                    self._parse_server_data_row()

                elif (self._result is not None and
                        self._result.status == PGRES_FATAL_ERROR):
                    # We've already choked for some reason.  Just discard
                    # tuples till we get to the end of the query.
                    self.buffer.consume_message()

                else:
                    # TODO
                    print("!!! server sent data (\"D\" message) without "
                          "prior row description (\"T\" message)")
                    self.buffer.consume_message()

            elif mtype == b'G':
                # Start Copy In
                raise NotImplementedError
            elif mtype == b'H':
                # Start Copy Out
                raise NotImplementedError
            elif mtype == b'W':
                # Start Copy Both
                raise NotImplementedError
            elif mtype == b'd':
                # Copy Data
                raise NotImplementedError
            elif mtype == b'c':
                # Copy Done
                raise NotImplementedError

            elif mtype == b'R':
                self._parse_server_authentication()

            else:
                raise RuntimeError(
                    'unsupported message type {!r}'.format(chr(mtype)))

        return DISPATCH_CONTINUE

    cdef _parse_server_authentication(self):
        cdef int status
        status = self.buffer.read_int32()
        if status == 0:
            # AuthenticationOk
            self._status = CONNECTION_OK
            self._async_status = PGASYNC_READY
            self._result = Result.new(PGRES_COMMAND_OK)
            self._push_result()
        else:
            raise RuntimeError(
                'unsupported status {} for Authentication (R) '
                'message'.format(status))

    cdef _parse_server_parameter_status(self):
        key = self.buffer.read_cstr().decode()
        val = self.buffer.read_cstr().decode()
        self._settings[key] = val

    cdef _parse_server_backend_key_data(self):
        self._backend_pid = self.buffer.read_int32()
        self._backend_secret = self.buffer.read_int32()

    cdef _parse_server_parameter_description(self):
        cdef Result result = Result.new(PGRES_COMMAND_OK)
        result.parameters_desc = self.buffer.consume_message()
        self._result = result

        if self._query_class == PGQUERY_DESCRIBE:
            self._async_status = PGASYNC_READY
            self._push_result()
            self._async_status = PGASYNC_BUSY

    cdef _parse_server_ready_for_query(self):
        cdef char status = self.buffer.read_byte()

        if status == b'I':
            self._xact_status = PQTRANS_IDLE
        elif status == b'T':
            self._xact_status = PQTRANS_INTRANS
        elif status == b'E':
            self._xact_status = PQTRANS_INERROR
        else:
            self._xact_status = PQTRANS_UNKNOWN

    cdef _parse_server_row_description(self):
        cdef Result result

        # TODO: look at fe-protocol3.c:getRowDescriptions

        if self._query_class == PGQUERY_DESCRIBE:
            if self._result:
                result = self._result
            else:
                result = Result.new(PGRES_COMMAND_OK)
        else:
            result = Result.new(PGRES_TUPLES_OK)

        result.row_desc = self.buffer.consume_message()
        self._result = result

        if self._query_class == PGQUERY_DESCRIBE:
            self._async_status = PGASYNC_READY
            self._push_result()

    cdef _parse_server_data_row(self):
        # See fe-protocol3.c:getAnotherTuple
        assert self._result is not None

        if self._result.rows is None:
            self._result.rows = []

        self._result.rows.append(self.buffer.consume_message())

    cdef _parse_server_error_response(self, is_error):
        cdef:
            char code
            bytes message
            dict parsed = {}
            Result res

        while True:
            code = self.buffer.read_byte()
            if code == 0:
                break

            message = self.buffer.read_cstr()

            parsed[chr(code)] = message.decode()

        if is_error:
            res = Result.new(PGRES_FATAL_ERROR)
            res.err_fields = parsed
            self._result = res
        # else:
        # TODO: process notices

    cdef _fatal_error(self, exc):
        try:
            if self.transport is not None:
                self.transport.close()
            self._state = CONNECTION_BAD
        finally:
            self._on_fatal_error(exc)

    cdef _push_result(self):
        cdef Result result
        if self._async_status != PGASYNC_READY:
            raise RuntimeError('result is not ready')
        if self._result is None:
            raise RuntimeError('no result to push')

        result = self._result
        self._result = None
        self._async_status = PGASYNC_IDLE
        self._on_result(result)

    cdef _sync(self):
        if self._async_status != PGASYNC_IDLE:
            raise RuntimeError('cannot sync; status is non-idle')
        self._async_status = PGASYNC_BUSY
        buf = WriteBuffer.new_message(b'S')
        buf.end_message()
        self._write(buf)

    cdef _ensure_ready_state(self):
        if self._async_status != PGASYNC_IDLE:
            raise RuntimeError('another command is already in progress')

        if self._status != CONNECTION_OK:
            raise RuntimeError('no connection to the server')

    # Cython API for subclasses:

    cdef _open(self):
        if self._status != CONNECTION_BAD:
            raise RuntimeError('already connected')

        self._async_status = PGASYNC_BUSY

        # Assemble a startup message
        buf = WriteBuffer()

        # protocol version
        buf.write_int16(3)
        buf.write_int16(0)

        buf.write_bytes(b'client_encoding')
        buf.write_bytes(b"'utf-8'")

        if self._user:
            buf.write_bytes(b'user')
            buf.write_str(self._user, self._encoding)

        buf.write_bytes(b'')

        # Send the buffer
        outbuf = WriteBuffer()
        outbuf.write_int32(buf.len() + 4)
        outbuf.write_buffer(buf)
        self._write(outbuf)

    cdef _query(self, str query):
        cdef WriteBuffer buf

        self._ensure_ready_state()

        buf = WriteBuffer.new_message(b'Q')
        buf.write_str(query, self._encoding)
        buf.end_message()

        self._query_class = PGQUERY_SIMPLE
        self._async_status = PGASYNC_BUSY
        self._write(buf)

    cdef _parse(self, str stmt_name, str query):
        cdef WriteBuffer buf

        self._ensure_ready_state()

        buf = WriteBuffer.new_message(b'P')
        buf.write_str(stmt_name, self._encoding)
        buf.write_str(query, self._encoding)
        buf.write_int16(0)
        buf.end_message()
        self._write(buf)

        buf = WriteBuffer.new_message(b'S')
        buf.end_message()
        self._write(buf)

        self._query_class = PGQUERY_PREPARE
        self._async_status = PGASYNC_BUSY

    cdef _bind(self, str portal_name, str stmt_name, tuple args):
        cdef WriteBuffer buf

        self._ensure_ready_state()

        buf = WriteBuffer.new_message(b'B')
        buf.write_str(portal_name, self._encoding)
        buf.write_str(stmt_name, self._encoding)

        # Specify the number of parameter codes:
        #   1 - the specified format code is applied to all parameters
        buf.write_int16(1)

        # Parameters format:
        #   0 - text
        buf.write_int16(0)

        # The number of parameters
        buf.write_int16(len(args))

        for arg in args:
            if arg is None:
                # Encode NULL
                buf.write_int32(1)
                buf.write_byte(-1)
            else:
                s = str(arg)
                buf.write_int32(len(s) + 1)
                buf.write_str(s, self._encoding)

        # Specify the result encoding
        #   1 - the specified format code is applied to all result columns
        buf.write_int16(1)
        # Result format: 1 - binary
        buf.write_int16(1)

        buf.end_message()
        self._write(buf)

        self._query_class = PGQUERY_PREPARE
        self._async_status = PGASYNC_BUSY

    cdef _describe(self, str name, bint is_portal):
        cdef WriteBuffer buf

        self._ensure_ready_state()

        buf = WriteBuffer.new_message(b'D')

        if is_portal:
            buf.write_byte(b'P')
        else:
            buf.write_byte(b'S')

        buf.write_str(name, self._encoding)
        buf.end_message()
        self._write(buf)

        buf = WriteBuffer.new_message(b'S')
        buf.end_message()
        self._write(buf)

        self._query_class = PGQUERY_DESCRIBE
        self._async_status = PGASYNC_BUSY

    cdef _on_result(self, Result result):
        pass

    cdef _on_fatal_error(self, exc):
        pass

    # asyncio callbacks:

    def data_received(self, data):
        self.buffer.feed_data(data)
        self._read_server_messages()

    def connection_made(self, transport):
        self.transport = transport

        try:
            self._open()
        except Exception as ex:
            self._fatal_error(ex)


cdef enum ProtocolState:
    STATE_NOT_CONNECTED = 0
    STATE_READY = 10

    STATE_PREPARE_BIND = 20
    STATE_PREPARE_DESCRIBE = 21

    STATE_QUERY = 30


cdef class PreparedStatement:
    cdef:
        readonly str name
        object row_desc
        object parameters_desc

    def __cinit__(self, name):
        self.name = name
        self.row_desc = self.parameters_desc = None

    def init(self):
        if self.row_desc is None:
            raise RuntimeError(
                'cannot init prepared statement: no row description')

        if self.parameters_desc is None:
            raise RuntimeError(
                'cannot init prepared statement: no parameters description')

        # TODO

    @staticmethod
    cdef _decode_parameters_desc(object desc):
        cdef:
            ReadBuffer reader
            int16_t nparams
            int32_t p_oid

        reader = ReadBuffer.new_message_parser(desc)
        nparams = reader.read_int16()

        for i from 0 <= i < nparams:
            p_oid = reader.read_int32()
            print(i, p_oid)

    @staticmethod
    cdef _decode_row_desc(object desc):
        cdef:
            ReadBuffer reader

            int16_t nfields

            bytes f_name
            int32_t f_table_oid
            int16_t f_column_num
            int32_t f_dt_oid
            int16_t f_dt_size
            int32_t f_dt_mod
            int16_t f_format

        reader = ReadBuffer.new_message_parser(desc)
        nfields = reader.read_int16()

        for i from 0 <= i < nfields:
            f_name = reader.read_cstr()
            f_table_oid = reader.read_int32()
            f_column_num = reader.read_int16()
            f_dt_oid = reader.read_int32()
            f_dt_size = reader.read_int16()
            f_dt_mod = reader.read_int32()
            f_format = reader.read_int16()

            print(f_name, f_table_oid, f_column_num, f_dt_oid,
                  f_dt_size, f_dt_mod, f_format)


cdef class BaseProtocol(CoreProtocol):

    cdef:
        object _loop

        object _connect_waiter
        object _waiter

        ProtocolState _state

        PreparedStatement _prepared_stmt

    def __init__(self, connect_waiter, user, loop):
        CoreProtocol.__init__(self, user=user)
        self._loop = loop

        self._connect_waiter = connect_waiter
        self._waiter = None
        self._state = STATE_NOT_CONNECTED

        self._prepared_stmt = None

    def query(self, query, waiter):
        self._start_state(STATE_QUERY)
        self._waiter = waiter
        self._query(query)

    def prepare(self, name, query, waiter):
        self._start_state(STATE_PREPARE_BIND)
        if self._prepared_stmt is not None:
            raise RuntimeError('another prepared statement is set')

        self._prepared_stmt = PreparedStatement(name)

        self._waiter = waiter
        self._parse(name, query)

    cdef _start_state(self, ProtocolState state):
        if self._state != STATE_READY:
            raise RuntimeError('"ready" state expected')
        if self._waiter is not None:
            raise RuntimeError('waiter is set in "ready" state')
        self._state = state

    cdef _on_result(self, Result result):
        cdef ProtocolState old_state = self._state

        if self._state == STATE_NOT_CONNECTED:
            if self._connect_waiter is None:
                raise RuntimeError(
                    'received connection result without connect_waiter set')
            self._connect_waiter.set_result(None)
            self._connect_waiter = None
            self._state = STATE_READY
            return

        if self._waiter is None:
            raise RuntimeError(
                'received result without a Future wating for it')

        if self._waiter.cancelled():
            # discard the result
            self._state = STATE_READY
            self._waiter = None
            return

        if result.status == PGRES_FATAL_ERROR:
            msg = '\n'.join(['{}: {}'.format(k, v)
                for k, v in result.err_fields.items()])
            exc = Exception(msg)
            self._waiter.set_exception(exc)
            self._state = STATE_READY
            return

        if self._state == STATE_QUERY:
            self._waiter.set_result(1)
            self._state = STATE_READY

        elif self._state == STATE_PREPARE_BIND:
            self._state = STATE_PREPARE_DESCRIBE
            self._describe(self._prepared_stmt.name, 0)

        elif self._state == STATE_PREPARE_DESCRIBE:
            if result.parameters_desc is not None:
                self._prepared_stmt.parameters_desc = result.parameters_desc
            if result.row_desc is not None:
                self._prepared_stmt.row_desc = result.row_desc
            if (self._prepared_stmt.row_desc is not None and
                    self._prepared_stmt.parameters_desc is not None):
                self._waiter.set_result(self._prepared_stmt)

                PreparedStatement._decode_parameters_desc(self._prepared_stmt.parameters_desc)
                PreparedStatement._decode_row_desc(self._prepared_stmt.row_desc)

                self._prepared_stmt = None
                self._state = STATE_READY
            else:
                # We keep the same state.
                return

        else:
            raise RuntimeError(
                'unknown state {} in on_result'.format(self._state))

        if self._state == old_state:
            raise RuntimeError('state was not updated in on_result')

        if self._state == STATE_READY:
            self._waiter = None


class Protocol(BaseProtocol, asyncio.Protocol):
    pass

