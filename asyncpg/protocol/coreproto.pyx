# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef class CoreProtocol:

    def __init__(self, con_args):
        self.buffer = ReadBuffer()
        self.con_args = con_args
        self.transport = None
        self.con_status = CONNECTION_BAD
        self.state = PROTOCOL_IDLE
        self.xact_status = PQTRANS_IDLE
        self.encoding = 'utf-8'

        self._skip_discard = False

        self._reset_result()

    cdef _write(self, buf):
        self.transport.write(memoryview(buf))

    cdef inline _write_sync_message(self):
        self.transport.write(SYNC_MESSAGE)

    cdef _read_server_messages(self):
        cdef:
            char mtype
            ProtocolState state

        while self.buffer.has_message() == 1:
            mtype = self.buffer.get_message_type()
            state = self.state

            try:
                if state == PROTOCOL_AUTH:
                    self._process__auth(mtype)

                elif state == PROTOCOL_PREPARE:
                    self._process__prepare(mtype)

                elif state == PROTOCOL_BIND_EXECUTE:
                    self._process__bind_execute(mtype)

                elif state == PROTOCOL_EXECUTE:
                    self._process__bind_execute(mtype)

                elif state == PROTOCOL_BIND:
                    self._process__bind(mtype)

                elif state == PROTOCOL_CLOSE_STMT_PORTAL:
                    self._process__close_stmt_portal(mtype)

                elif state == PROTOCOL_SIMPLE_QUERY:
                    self._process__simple_query(mtype)

                elif state == PROTOCOL_ERROR_CONSUME:
                    # Error in protocol (on asyncpg side);
                    # discard all messages until sync message

                    if mtype == b'Z':
                        # Sync point, self to push the result
                        if self.result_type != RESULT_FAILED:
                            self.result_type = RESULT_FAILED
                            self.result = RuntimeError(
                                'unknown error in protocol implementation')

                        self._push_result()

                    else:
                        self.buffer.consume_message()

                else:
                    raise RuntimeError(
                        'protocol is in an unknown state {}'.format(state))

                if mtype == b'S':
                    # ParameterStatus
                    self._parse_msg_parameter_status()

            except Exception as ex:
                self.result_type = RESULT_FAILED
                self.result = ex

                if mtype == b'Z':
                    self._push_result()
                else:
                    self.state = PROTOCOL_ERROR_CONSUME

            finally:
                if self._skip_discard:
                    self._skip_discard = False
                else:
                    self.buffer.discard_message()

    cdef _process__auth(self, char mtype):
        if mtype == b'R':
            # Authentication...
            self._parse_msg_authentication()
            if self.result_type != RESULT_OK:
                self.con_status = CONNECTION_BAD
                self._push_result()
                self.transport.close()

        elif mtype == b'K':
            # BackendKeyData
            self._parse_msg_backend_key_data()

        elif mtype == b'E':
            # ErrorResponse
            self.con_status = CONNECTION_BAD
            self._parse_msg_error_response(True)
            self._push_result()

        elif mtype == b'Z':
            # ReadyForQuery
            self._parse_msg_ready_for_query()
            self.con_status = CONNECTION_OK
            self._push_result()

    cdef _process__prepare(self, char mtype):
        if mtype == b't':
            # Parameters description
            self.result_param_desc = self.buffer.consume_message().as_bytes()

        elif mtype == b'1':
            # ParseComplete
            self.buffer.consume_message()

        elif mtype == b'T':
            # Row description
            self.result_row_desc = self.buffer.consume_message().as_bytes()

        elif mtype == b'E':
            # ErrorResponse
            self._parse_msg_error_response(True)

        elif mtype == b'Z':
            # ReadyForQuery
            self._parse_msg_ready_for_query()
            self._push_result()

        elif mtype == b'n':
            # NoData
            self.buffer.consume_message()

    cdef _process__bind_execute(self, char mtype):
        if mtype == b'D':
            # DataRow
            self._parse_data_msgs()

        elif mtype == b's':
            # PortalSuspended
            self.buffer.consume_message()

        elif mtype == b'C':
            # CommandComplete
            self.result_execute_completed = True
            self._parse_msg_command_complete()

        elif mtype == b'E':
            # ErrorResponse
            self._parse_msg_error_response(True)

        elif mtype == b'2':
            # BindComplete
            self.buffer.consume_message()

        elif mtype == b'Z':
            # ReadyForQuery
            self._parse_msg_ready_for_query()
            self._push_result()

        elif mtype == b'I':
            # EmptyQueryResponse
            self.buffer.consume_message()

    cdef _process__bind(self, char mtype):
        if mtype == b'E':
            # ErrorResponse
            self._parse_msg_error_response(True)

        elif mtype == b'2':
            # BindComplete
            self.buffer.consume_message()

        elif mtype == b'Z':
            # ReadyForQuery
            self._parse_msg_ready_for_query()
            self._push_result()

    cdef _process__close_stmt_portal(self, char mtype):
        if mtype == b'E':
            # ErrorResponse
            self._parse_msg_error_response(True)

        elif mtype == b'3':
            # CloseComplete
            self.buffer.consume_message()

        elif mtype == b'Z':
            # ReadyForQuery
            self._parse_msg_ready_for_query()
            self._push_result()

    cdef _process__simple_query(self, char mtype):
        if mtype in {b'D', b'I', b'N', b'T'}:
            # 'D' - DataRow
            # 'I' - EmptyQueryResponse
            # 'N' - NoticeResponse
            # 'T' - RowDescription
            self.buffer.consume_message()

        elif mtype == b'E':
            # ErrorResponse
            self._parse_msg_error_response(True)

        elif mtype == b'Z':
            # ReadyForQuery
            self._parse_msg_ready_for_query()
            self._push_result()

        elif mtype == b'C':
            # CommandComplete
            self._parse_msg_command_complete()

        else:
            # We don't really care about COPY IN etc
            self.buffer.consume_message()

    cdef _parse_msg_command_complete(self):
        cdef:
            char* cbuf
            int32_t cbuf_len

        cbuf = self.buffer.try_consume_message(&cbuf_len)
        if cbuf != NULL and cbuf_len > 0:
            msg = cpython.PyBytes_FromStringAndSize(cbuf, cbuf_len - 1)
        else:
            msg = self.buffer.read_cstr()
        self.result_status_msg = msg

    cdef _parse_data_msgs(self):
        cdef:
            ReadBuffer buf = self.buffer
            list rows
            decode_row_method decoder = <decode_row_method>self._decode_row

            char* cbuf
            int32_t cbuf_len
            object row
            Memory mem

        IF DEBUG:
            if buf.get_message_type() != b'D':
                raise RuntimeError(
                    '_parse_data_msgs: first message is not "D"')

            if type(self.result) is not list:
                raise RuntimeError(
                    '_parse_data_msgs: result is not a list, but {!r}'.
                    format(self.result))

        rows = self.result
        while True:
            cbuf = buf.try_consume_message(&cbuf_len)
            if cbuf != NULL:
                row = decoder(self, cbuf, cbuf_len)
            else:
                mem = buf.consume_message()
                row = decoder(self, mem.buf, mem.length)

            cpython.PyList_Append(rows, row)

            if not buf.has_message() or buf.get_message_type() != b'D':
                self._skip_discard = True
                return

    cdef _parse_msg_backend_key_data(self):
        self.backend_pid = self.buffer.read_int32()
        self.backend_secret = self.buffer.read_int32()

    cdef _parse_msg_parameter_status(self):
        name = self.buffer.read_cstr()
        name = name.decode(self.encoding)

        val = self.buffer.read_cstr()
        val = val.decode(self.encoding)

        self._set_server_parameter(name, val)

    cdef _parse_msg_authentication(self):
        cdef int status
        status = self.buffer.read_int32()
        if status != 0:
            self.result_type = RESULT_FAILED
            self.result = apg_exc.InterfaceError(
                'unsupported status {} for Authentication (R) '
                'message'.format(status))
        else:
            # 0 == AuthenticationOk
            self.result_type = RESULT_OK
        self.buffer.consume_message()


    cdef _parse_msg_ready_for_query(self):
        cdef char status = self.buffer.read_byte()

        if status == b'I':
            self.xact_status = PQTRANS_IDLE
        elif status == b'T':
            self.xact_status = PQTRANS_INTRANS
        elif status == b'E':
            self.xact_status = PQTRANS_INERROR
        else:
            self.xact_status = PQTRANS_UNKNOWN

    cdef _parse_msg_error_response(self, is_error):
        cdef:
            char code
            bytes message
            dict parsed = {}

        while True:
            code = self.buffer.read_byte()
            if code == 0:
                break

            message = self.buffer.read_cstr()

            parsed[chr(code)] = message.decode()

        if is_error:
            self.result_type = RESULT_FAILED
            self.result = parsed

    cdef _push_result(self):
        try:
            self._on_result()
        finally:
            self._set_state(PROTOCOL_IDLE)
            self._reset_result()

    cdef _reset_result(self):
        self.result_type = RESULT_OK
        self.result = None
        self.result_param_desc = None
        self.result_row_desc = None
        self.result_status_msg = None
        self.result_execute_completed = False

    cdef _set_state(self, ProtocolState new_state):
        if new_state == PROTOCOL_IDLE:
            if self.state == PROTOCOL_FAILED:
                raise RuntimeError(
                    'cannot switch to "idle" state; '
                    'protocol is in the "failed" state')
            elif self.state == PROTOCOL_IDLE:
                raise RuntimeError(
                    'protocol is already in the "idle" state')
            else:
                self.state = new_state

        elif new_state == PROTOCOL_FAILED:
            self.state = PROTOCOL_FAILED

        else:
            if self.state == PROTOCOL_IDLE:
                self.state = new_state

            elif self.state == PROTOCOL_FAILED:
                raise RuntimeError(
                    'cannot switch to state {}; '
                    'protocol is in the "failed" state'.format(new_state))
            else:
                raise RuntimeError(
                    'cannot switch to state {}; '
                    'another operation ({}) is in progress'.format(
                        new_state, self.state))

    cdef _ensure_connected(self):
        if self.con_status != CONNECTION_OK:
            raise RuntimeError('not connected')

    cdef WriteBuffer _build_bind_message(self, str portal_name,
                                         str stmt_name,
                                         WriteBuffer bind_data):
        cdef WriteBuffer buf

        buf = WriteBuffer.new_message(b'B')
        buf.write_str(portal_name, self.encoding)
        buf.write_str(stmt_name, self.encoding)

        # Arguments
        buf.write_buffer(bind_data)

        buf.end_message()
        return buf

    # API for subclasses

    cdef _connect(self):
        cdef:
            WriteBuffer buf
            WriteBuffer outbuf

        if self.con_status != CONNECTION_BAD:
            raise RuntimeError('already connected')

        self._set_state(PROTOCOL_AUTH)
        self.con_status = CONNECTION_STARTED

        # Assemble a startup message
        buf = WriteBuffer()

        # protocol version
        buf.write_int16(3)
        buf.write_int16(0)

        buf.write_bytestring(b'client_encoding')
        buf.write_bytestring("'{}'".format(self.encoding).encode('ascii'))

        for param in self.con_args:
            buf.write_str(param, self.encoding)
            buf.write_str(self.con_args[param], self.encoding)

        buf.write_bytestring(b'')

        # Send the buffer
        outbuf = WriteBuffer()
        outbuf.write_int32(buf.len() + 4)
        outbuf.write_buffer(buf)
        self._write(outbuf)

    cdef _prepare(self, str stmt_name, str query):
        cdef:
            WriteBuffer packet
            WriteBuffer buf

        self._ensure_connected()
        self._set_state(PROTOCOL_PREPARE)

        packet = WriteBuffer.new()

        buf = WriteBuffer.new_message(b'P')
        buf.write_str(stmt_name, self.encoding)
        buf.write_str(query, self.encoding)
        buf.write_int16(0)
        buf.end_message()
        packet.write_buffer(buf)

        buf = WriteBuffer.new_message(b'D')
        buf.write_byte(b'S')
        buf.write_str(stmt_name, self.encoding)
        buf.end_message()
        packet.write_buffer(buf)

        packet.write_bytes(SYNC_MESSAGE)

        self.transport.write(memoryview(packet))

    cdef _bind_execute(self, str portal_name, str stmt_name,
                       WriteBuffer bind_data, int32_t limit):

        cdef WriteBuffer buf

        self._ensure_connected()
        self._set_state(PROTOCOL_BIND_EXECUTE)

        self.result = []

        buf = self._build_bind_message(portal_name, stmt_name, bind_data)
        self._write(buf)

        buf = WriteBuffer.new_message(b'E')
        buf.write_str(portal_name, self.encoding)  # name of the portal
        buf.write_int32(limit)  # number of rows to return; 0 - all
        buf.end_message()
        self._write(buf)

        self._write_sync_message()

    cdef _execute(self, str portal_name, int32_t limit):
        cdef WriteBuffer buf

        self._ensure_connected()
        self._set_state(PROTOCOL_EXECUTE)

        self.result = []

        buf = WriteBuffer.new_message(b'E')
        buf.write_str(portal_name, self.encoding)  # name of the portal
        buf.write_int32(limit)  # number of rows to return; 0 - all
        buf.end_message()
        self._write(buf)
        self._write_sync_message()

    cdef _bind(self, str portal_name, str stmt_name,
               WriteBuffer bind_data):

        cdef WriteBuffer buf

        self._ensure_connected()
        self._set_state(PROTOCOL_BIND)

        buf = self._build_bind_message(portal_name, stmt_name, bind_data)
        self._write(buf)
        self._write_sync_message()

    cdef _close(self, str name, bint is_portal):
        cdef WriteBuffer buf

        self._ensure_connected()
        self._set_state(PROTOCOL_CLOSE_STMT_PORTAL)

        buf = WriteBuffer.new_message(b'C')

        if is_portal:
            buf.write_byte(b'P')
        else:
            buf.write_byte(b'S')

        buf.write_str(name, self.encoding)
        buf.end_message()
        self._write(buf)

        self._write_sync_message()

    cdef _simple_query(self, str query):
        cdef WriteBuffer buf
        self._ensure_connected()
        self._set_state(PROTOCOL_SIMPLE_QUERY)
        buf = WriteBuffer.new_message(b'Q')
        buf.write_str(query, self.encoding)
        buf.end_message()
        self._write(buf)

    cdef _terminate(self):
        cdef WriteBuffer buf
        self._ensure_connected()
        buf = WriteBuffer.new_message(b'X')
        buf.end_message()
        self._write(buf)

    cdef _decode_row(self, const char* buf, int32_t buf_len):
        pass

    cdef _set_server_parameter(self, name, val):
        pass

    cdef _on_result(self):
        pass

    cdef _on_connection_lost(self, exc):
        pass

    # asyncio callbacks:

    def data_received(self, data):
        self.buffer.feed_data(data)
        self._read_server_messages()

    def connection_made(self, transport):
        self.transport = transport

        sock = transport.get_extra_info('socket')
        if sock is not None and sock.family != socket.AF_UNIX:
            sock.setsockopt(socket.IPPROTO_TCP,
                            socket.TCP_NODELAY, 1)

        try:
            self._connect()
        except Exception as ex:
            transport.abort()
            self.con_status = CONNECTION_BAD
            self._set_state(PROTOCOL_FAILED)
            self._on_error(ex)

    def connection_lost(self, exc):
        self.con_status = CONNECTION_BAD
        self._set_state(PROTOCOL_FAILED)
        self._on_connection_lost(exc)


cdef bytes SYNC_MESSAGE = bytes(WriteBuffer.new_message(b'S').end_message())
