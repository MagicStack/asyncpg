# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


cdef class Memory:
    cdef:
        char* buf
        object owner
        int length

    cdef as_bytes(self)

    @staticmethod
    cdef inline Memory new(char* buf, object owner, int length)


cdef class WriteBuffer:
    cdef:
        # Preallocated small buffer
        bint _smallbuf_inuse
        char _smallbuf[_BUFFER_INITIAL_SIZE]

        char *_buf

        # Allocated size
        size_t _size

        # Length of data in the buffer
        size_t _length

        # Number of memoryviews attached to the buffer
        int _view_count

        # True is start_message was used
        bint _message_mode

    cdef inline _check_readonly(self)
    cdef inline len(self)
    cdef inline _ensure_alloced(self, size_t extra_length)
    cdef _reallocate(self, new_size)
    cdef inline start_message(self, char type)
    cdef inline end_message(self)
    cdef write_buffer(self, WriteBuffer buf)
    cdef write_byte(self, char b)
    cdef write_bytes(self, bytes data)
    cdef write_bytestring(self, bytes string)
    cdef write_str(self, str string, str encoding)
    cdef write_cstr(self, char *data, ssize_t len)
    cdef write_int16(self, int16_t i)
    cdef write_int32(self, int32_t i)
    cdef write_int64(self, int64_t i)
    cdef write_float(self, float f)
    cdef write_double(self, double d)

    @staticmethod
    cdef WriteBuffer new_message(char type)

    @staticmethod
    cdef WriteBuffer new()


cdef class ReadBuffer:
    cdef:
        # A deque of buffers (bytes objects)
        object _bufs
        object _bufs_append
        object _bufs_popleft

        # A pointer to the first buffer in `_bufs`
        bytes _buf0

        # A pointer to the previous first buffer
        # (used to prolong the life of _buf0 when using
        # methods like _try_read_bytes)
        bytes _buf0_prev

        # Number of buffers in `_bufs`
        int32_t _bufs_len

        # A read position in the first buffer in `_bufs`
        int32_t _pos0

        # Length of the first buffer in `_bufs`
        int32_t _len0

        # A total number of buffered bytes in ReadBuffer
        int32_t _length

        char _current_message_type
        int _current_message_len
        int32_t _current_message_len_unread
        bint _current_message_ready

    cdef feed_data(self, data)
    cdef inline _ensure_first_buf(self)
    cdef _switch_to_next_buf(self)
    cdef inline read_byte(self)
    cdef inline char* _try_read_bytes(self, int nbytes)
    cdef inline read(self, int nbytes)
    cdef inline read_bytes(self, ssize_t n)
    cdef inline read_int32(self)
    cdef inline read_int16(self)
    cdef inline read_cstr(self)
    cdef int32_t has_message(self) except -1
    cdef inline char* try_consume_message(self, int32_t* len)
    cdef Memory consume_message(self)
    cdef discard_message(self)
    cdef inline _discard_message(self)
    cdef inline char get_message_type(self)
    cdef inline int32_t get_message_length(self)

    @staticmethod
    cdef ReadBuffer new_message_parser(object data)


cdef class FastReadBuffer:
    cdef:
        const char* buf
        size_t len

    cdef inline const char* read(self, size_t n) except NULL
    cdef inline const char* read_all(self)
    cdef inline FastReadBuffer slice_from(self, FastReadBuffer source,
                                          size_t len)
    cdef _raise_ins_err(self, size_t n, size_t len)

    @staticmethod
    cdef FastReadBuffer new()
