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


cdef bytes SYNC_MESSAGE = bytes(WriteBuffer.new_message(b'S').end_message())


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
        str _password
        str _dbname
        str _encoding

        ####### Connection State:

        ConnectionSettings _settings

        int _backend_pid
        int _backend_secret

        # result being constructed
        Result            _result

        ConnectionStatus  _status
        AsyncStatus       _async_status
        QueryClass        _query_class
        TransactionStatus _xact_status

        WriteBuffer _after_sync

    def __init__(self, user, password, dbname):
        self.buffer = ReadBuffer()
        self.transport = None

        self._user = user
        self._password = password
        self._dbname = dbname

        self._encoding = 'utf-8'
        self._settings = ConnectionSettings()
        self._backend_pid = 0
        self._backend_secret = 0

        self._result = None

        self._status = CONNECTION_BAD
        self._async_status = PGASYNC_IDLE
        self._xact_status = PQTRANS_IDLE

        self._after_sync = None

    cdef inline _write(self, WriteBuffer buf):
        self.transport.write(memoryview(buf))

    cdef inline _write_sync_message(self):
        self.transport.write(SYNC_MESSAGE)

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

            elif mtype == b'Z':
                # TODO
                self.buffer.consume_message()

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
                if self._status == CONNECTION_STARTED:
                    self._status = CONNECTION_BAD
                self._parse_server_error_response(True)
                self._async_status = PGASYNC_READY
                self._push_result()

            elif mtype == b'Z':
                # Backend is ready for new query
                self._parse_server_ready_for_query()

                if self._status == CONNECTION_STARTED:
                    self._status = CONNECTION_OK
                    self._result = Result.new(PGRES_COMMAND_OK)
                    self._async_status = PGASYNC_READY
                    self._push_result()

                elif self._after_sync is not None:
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
        if status != 0:
            # 0 == AuthenticationOk
            raise RuntimeError(
                'unsupported status {} for Authentication (R) '
                'message'.format(status))
        self.buffer.consume_message()

    cdef _parse_server_parameter_status(self):
        key = self.buffer.read_cstr().decode()
        val = self.buffer.read_cstr().decode()
        self._settings.add_setting(key, val)

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
        self._write_sync_message()

    cdef _ensure_ready_state(self):
        if self._async_status != PGASYNC_IDLE:
            raise RuntimeError('another command is already in progress')

        if self._status != CONNECTION_OK:
            raise RuntimeError('no connection to the server')

    # Cython API for subclasses:

    cdef _open(self):
        if self._status != CONNECTION_BAD:
            raise RuntimeError('already connected')

        self._status = CONNECTION_STARTED
        self._async_status = PGASYNC_BUSY

        # Assemble a startup message
        buf = WriteBuffer()

        # protocol version
        buf.write_int16(3)
        buf.write_int16(0)

        buf.write_bytestring(b'client_encoding')
        buf.write_bytestring("'{}'".format(self._encoding).encode('ascii'))

        if self._user:
            buf.write_bytestring(b'user')
            buf.write_str(self._user, self._encoding)

        if self._password:
            buf.write_bytestring(b'password')
            buf.write_str(self._password, self._encoding)

        if self._dbname:
            buf.write_bytestring(b'dbname')
            buf.write_str(self._dbname, self._encoding)

        buf.write_bytestring(b'')

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

        self._write_sync_message()

        self._query_class = PGQUERY_PREPARE
        self._async_status = PGASYNC_BUSY

    cdef _bind(self, str portal_name, str stmt_name, WriteBuffer bind_data):
        cdef WriteBuffer buf

        self._ensure_ready_state()

        buf = WriteBuffer.new_message(b'B')
        buf.write_str(portal_name, self._encoding)
        buf.write_str(stmt_name, self._encoding)

        # Arguments
        buf.write_buffer(bind_data)

        buf.end_message()
        self._write(buf)

        buf = WriteBuffer.new_message(b'E')
        buf.write_str(portal_name, self._encoding)  # name of the portal
        buf.write_int32(0)  # number of rows to return; 0 - all
        buf.end_message()
        self._write(buf)

        self._write_sync_message()

        self._result = Result.new(PGRES_TUPLES_OK)
        self._query_class = PGQUERY_EXTENDED
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

        self._write_sync_message()

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
