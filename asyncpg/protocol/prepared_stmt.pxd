cdef class PreparedStatementState:
    cdef:
        readonly str name
        readonly str query
        list         row_desc
        list         parameters_desc

        BaseProtocol protocol
        ConnectionSettings settings

        int16_t      args_num
        bint         have_text_args
        tuple        args_codecs

        int16_t      cols_num
        dict         cols_mapping
        bint         have_text_cols
        tuple        rows_codecs

    cdef _encode_bind_msg(self, args)
    cdef _ensure_rows_decoder(self)
    cdef _ensure_args_encoder(self)
    cdef _set_row_desc(self, object desc)
    cdef _set_args_desc(self, object desc)
    cdef _decode_row(self, Memory mem)
