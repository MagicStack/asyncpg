# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


@cython.final
cdef class PreparedStatementState:

    def __cinit__(self, str name, str query, BaseProtocol protocol):
        self.name = name
        self.query = query
        self.protocol = protocol
        self.settings = protocol.settings
        self.row_desc = self.parameters_desc = None
        self.args_codecs = self.rows_codecs = None
        self.args_num = self.cols_num = 0
        self.cols_desc = None
        self.closed = False
        self.refs = 0
        self.buffer = FastReadBuffer.new()

    def _get_parameters(self):
        cdef Codec codec

        result = []
        for oid in self.parameters_desc:
            codec = self.settings.get_data_codec(oid)
            if codec is None:
                raise RuntimeError
            result.append(apg_types.Type(
                oid, codec.name, codec.kind, codec.schema))

        return tuple(result)

    def _get_attributes(self):
        cdef Codec codec

        result = []
        for d in self.row_desc:
            name = d[0]
            oid = d[3]

            codec = self.settings.get_data_codec(oid)
            if codec is None:
                raise RuntimeError

            name = name.decode(self.settings._encoding)

            result.append(
                apg_types.Attribute(name,
                    apg_types.Type(oid, codec.name, codec.kind, codec.schema)))

        return tuple(result)

    def _init_types(self):
        cdef:
            Codec codec
            set result = set()

        if self.parameters_desc:
            for p_oid in self.parameters_desc:
                codec = self.settings.get_data_codec(<uint32_t>p_oid)
                if codec is None or not codec.has_encoder():
                    result.add(p_oid)

        if self.row_desc:
            for rdesc in self.row_desc:
                codec = self.settings.get_data_codec(<uint32_t>(rdesc[3]))
                if codec is None or not codec.has_decoder():
                    result.add(rdesc[3])

        if len(result):
            return result
        else:
            return True

    def attach(self):
        self.refs += 1

    def detach(self):
        self.refs -= 1

    def mark_closed(self):
        self.closed = True

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
            arg = args[idx]
            if arg is None:
                writer.write_int32(-1)
            else:
                codec = <Codec>(self.args_codecs[idx])
                codec.encode(self.settings, writer, arg)

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
            list cols_names
            object cols_mapping
            tuple row
            int oid
            Codec codec
            list codecs

        if self.cols_desc is not None:
            return

        if self.cols_num == 0:
            self.cols_desc = record.ApgRecordDesc_New({}, ())
            return

        cols_mapping = collections.OrderedDict()
        cols_names = []
        codecs = []
        for i from 0 <= i < self.cols_num:
            row = self.row_desc[i]
            col_name = row[0].decode(self.settings._encoding)
            cols_mapping[col_name] = i
            cols_names.append(col_name)
            oid = row[3]
            codec = self.settings.get_data_codec(<uint32_t>oid)
            if codec is None or not codec.has_decoder():
                raise RuntimeError('no decoder for OID {}'.format(oid))
            if not codec.is_binary():
                self.have_text_cols = True

            codecs.append(codec)

        self.cols_desc = record.ApgRecordDesc_New(
            cols_mapping, tuple(cols_names))

        self.rows_codecs = tuple(codecs)

    cdef _ensure_args_encoder(self):
        cdef:
            int p_oid
            Codec codec
            list codecs = []

        if self.args_num == 0 or self.args_codecs is not None:
            return

        for i from 0 <= i < self.args_num:
            p_oid = self.parameters_desc[i]
            codec = self.settings.get_data_codec(<uint32_t>p_oid)
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

    cdef _decode_row(self, const char* cbuf, int32_t buf_len):
        cdef:
            Codec codec
            int16_t fnum
            int32_t flen
            object dec_row
            tuple rows_codecs = self.rows_codecs
            ConnectionSettings settings = self.settings
            int32_t i
            FastReadBuffer rbuf = self.buffer
            size_t bl

        rbuf.buf = cbuf
        rbuf.len = buf_len

        fnum = hton.unpack_int16(rbuf.read(2))

        if fnum != self.cols_num:
            raise RuntimeError(
                'number of columns in result ({}) is '
                'different from what was described ({})'.format(
                    fnum, self.cols_num))

        if rows_codecs is None or len(rows_codecs) < fnum:
            if fnum > 0:
                # It's OK to have no rows_codecs for empty records
                raise RuntimeError('invalid rows_codecs')

        dec_row = record.ApgRecord_New(self.cols_desc, fnum)
        for i in range(fnum):
            flen = hton.unpack_int32(rbuf.read(4))

            if flen == -1:
                val = None
            else:
                # Clamp buffer size to that of the reported field length
                # to make sure that codecs can rely on read_all() working
                # properly.
                bl = rbuf.len
                if flen > bl:
                    # Check for overflow
                    rbuf._raise_ins_err(flen, bl)
                rbuf.len = flen
                codec = <Codec>cpython.PyTuple_GET_ITEM(rows_codecs, i)
                val = codec.decode(settings, rbuf)
                if rbuf.len != 0:
                    raise BufferError(
                        'unexpected trailing {} bytes in buffer'.format(
                            rbuf.len))
                rbuf.len = bl - flen

            cpython.Py_INCREF(val)
            record.ApgRecord_SET_ITEM(dec_row, i, val)

        if rbuf.len != 0:
            raise BufferError('unexpected trailing {} bytes in buffer'.format(
                rbuf.len))

        return dec_row


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
