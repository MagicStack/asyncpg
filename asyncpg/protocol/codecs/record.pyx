cdef inline record_encode_frame(ConnectionSettings settings, WriteBuffer buf,
                                WriteBuffer elem_data, uint32_t elem_count):
    buf.write_int32(4 + elem_data.len())
    # attribute count
    buf.write_int32(elem_count)
    # encoded attribute data
    buf.write_buffer(elem_data)
