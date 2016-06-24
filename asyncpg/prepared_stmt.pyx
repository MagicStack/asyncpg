cdef class PreparedStatementState:

    def __cinit__(self, str name, BaseProtocol protocol):
        self.name = name
        self.protocol = protocol
        self.settings = protocol._settings
        self.row_desc = self.parameters_desc = None
        self.args_codecs = self.rows_codecs = None
        self.args_num = self.cols_num = 0
        self.types_ready = False

    def _init_types(self):
        cdef:
            Codec codec
            set result = set()

        if self.parameters_desc:
            for p_oid in self.parameters_desc:
                codec = self.protocol._get_codec(<uint32_t>p_oid)
                if codec is None or not codec.has_encoder():
                    result.add(p_oid)

        if self.row_desc:
            for rdesc in self.row_desc:
                codec = self.protocol._get_codec(<uint32_t>(rdesc[3]))
                if codec is None or not codec.has_decoder():
                    result.add(rdesc[3])

        if len(result):
            return result
        else:
            self.types_ready = True
            return True

    cdef _encode_bind_msg(self, args):
        cdef:
            int idx
            WriteBuffer writer
            Codec codec

        self._ensure_args_encoder()
        self._ensure_rows_decoder()

        writer = WriteBuffer.new()

        if self.args_num != len(args):
            raise ValueError(
                'number of arguments ({}) does not match '
                'number of parameters ({})'.format(
                    len(args), self.args_num))

        if self.have_text_args:
            writer.write_int16(self.args_num)
            for idx from 0 <= idx < self.args_num:
                codec = <Codec>(self.args_codecs[idx])
                writer.write_int16(codec.format)
        else:
            # All arguments are in binary format
            writer.write_int32(0x00010001)

        writer.write_int16(self.args_num)

        for idx from 0 <= idx < self.args_num:
            codec = <Codec>(self.args_codecs[idx])
            codec.encode(self.settings, writer, args[idx])

        if self.have_text_cols:
            writer.write_int16(self.cols_num)
            for idx from 0 <= idx < self.cols_num:
                codec = <Codec>(self.rows_codecs[idx])
                writer.write_int16(codec.format)
        else:
            # All columns are in binary format
            writer.write_int32(0x00010001)

        return writer

    cdef _ensure_rows_decoder(self):
        cdef:
            int oid
            Codec codec
            list codecs = []

        if self.cols_num == 0:
            return

        for i from 0 <= i < self.cols_num:
            oid = self.row_desc[i][3]
            codec = self.protocol._get_codec(<uint32_t>oid)
            if codec is None or not codec.has_decoder():
                raise RuntimeError('no decoder for OID {}'.format(oid))
            if not codec.is_binary():
                self.have_text_cols = True

            codecs.append(codec)

        self.rows_codecs = tuple(codecs)

    cdef _ensure_args_encoder(self):
        cdef:
            int p_oid
            Codec codec
            list codecs = []

        if self.args_num == 0:
            return

        for i from 0 <= i < self.args_num:
            p_oid = self.parameters_desc[i]
            codec = self.protocol._get_codec(<uint32_t>p_oid)
            if codec is None or not codec.has_encoder():
                raise RuntimeError('no encoder for OID {}'.format(p_oid))
            if codec.type not in {}:
                self.have_text_args = True

            codecs.append(codec)

        self.args_codecs = tuple(codecs)

    cdef _set_row_desc(self, object desc):
        self.row_desc = _decode_row_desc(desc)
        self.cols_num = <int16_t>(len(self.row_desc))

    cdef _set_args_desc(self, object desc):
        self.parameters_desc = _decode_parameters_desc(desc)
        self.args_num = <int16_t>(len(self.parameters_desc))

    cdef _decode_rows(self, rows):
        cdef:
            Codec codec
            Py_buffer *pybuf
            char *cbuf
            int16_t fnum
            int32_t flen
            list result
            list dec_row
            int row_len

        result = []

        for row in rows:
            if not PyMemoryView_Check(row):
                raise RuntimeError('memoryview expected')

            dec_row = []
            row_len = len(row)

            pybuf = PyMemoryView_GET_BUFFER(row)

            cbuf = <char*>pybuf.buf
            fnum = hton.unpack_int16(cbuf)
            cbuf += 2

            if fnum != self.cols_num:
                raise RuntimeError(
                    'number of columns in result ({}) is '
                    'different from what was described ({})'.format(
                        fnum, self.cols_num))

            for i from 0 <= i < self.cols_num:
                flen = hton.unpack_int32(cbuf)
                cbuf += 4

                if flen == -1:
                    dec_row.append(None)
                    continue

                codec = <Codec>self.rows_codecs[i]

                dec_row.append(codec.decode(self.settings, cbuf, flen))
                cbuf += flen

                if cbuf - <char*>pybuf.buf > row_len:
                    raise RuntimeError('buffer overrun')

            result.append(dec_row)

        return result


cdef _decode_parameters_desc(object desc):
    cdef:
        ReadBuffer reader
        int16_t nparams
        int32_t p_oid
        list result = []

    reader = ReadBuffer.new_message_parser(desc)
    nparams = reader.read_int16()

    for i from 0 <= i < nparams:
        p_oid = reader.read_int32()
        result.append(p_oid)

    return result


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

        list result

    reader = ReadBuffer.new_message_parser(desc)
    nfields = reader.read_int16()
    result = []

    for i from 0 <= i < nfields:
        f_name = reader.read_cstr()
        f_table_oid = reader.read_int32()
        f_column_num = reader.read_int16()
        f_dt_oid = reader.read_int32()
        f_dt_size = reader.read_int16()
        f_dt_mod = reader.read_int32()
        f_format = reader.read_int16()

        result.append(
            (f_name, f_table_oid, f_column_num, f_dt_oid,
             f_dt_size, f_dt_mod, f_format))

    return result
