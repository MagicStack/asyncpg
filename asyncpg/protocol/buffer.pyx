from cpython cimport Py_buffer
from libc.string cimport memcpy


class BufferError(Exception):
    pass


@cython.no_gc_clear
@cython.freelist(_BUFFER_FREELIST_SIZE)
cdef class WriteBuffer:

    def __cinit__(self):
        self._smallbuf_inuse = True
        self._buf = self._smallbuf
        self._size = _BUFFER_INITIAL_SIZE
        self._length = 0
        self._message_mode = 0

    def __dealloc__(self):
        if self._buf is not NULL and not self._smallbuf_inuse:
            PyMem_Free(self._buf)
            self._buf = NULL
            self._size = 0

        if self._view_count:
            raise RuntimeError(
                'Deallocating buffer with attached memoryviews')

    def __getbuffer__(self, Py_buffer *buffer, int flags):
        self._view_count += 1

        PyBuffer_FillInfo(
            buffer, self, self._buf, self._length,
            1,  # read-only
            flags)

    def __releasebuffer__(self, Py_buffer *buffer):
        self._view_count -= 1

    cdef inline _check_readonly(self):
        if self._view_count:
            raise BufferError('the buffer is in read-only mode')

    cdef inline len(self):
        return self._length

    cdef inline _ensure_alloced(self, size_t extra_length):
        cdef size_t new_size = extra_length + self._length

        if new_size > self._size:
            self._reallocate(new_size)

    cdef _reallocate(self, new_size):
        cdef char *new_buf

        if new_size < _BUFFER_MAX_GROW:
            new_size = _BUFFER_MAX_GROW
        else:
            # Add a little extra
            new_size += _BUFFER_INITIAL_SIZE

        if self._smallbuf_inuse:
            new_buf = <char*>PyMem_Malloc(sizeof(char) * new_size)
            if new_buf is NULL:
                self._buf = NULL
                self._size = 0
                self._length = 0
                raise MemoryError
            memcpy(new_buf, self._buf, self._size)
            self._size = new_size
            self._buf = new_buf
            self._smallbuf_inuse = False
        else:
            new_buf = <char*>PyMem_Realloc(<void*>self._buf, new_size)
            if new_buf is NULL:
                PyMem_Free(self._buf)
                self._buf = NULL
                self._size = 0
                self._length = 0
                raise MemoryError
            self._buf = new_buf
            self._size = new_size

    cdef inline start_message(self, char type):
        if self._length != 0:
            raise BufferError('cannot start_message for a non-empty buffer')
        self._ensure_alloced(5)
        self._message_mode = 1
        self._buf[0] = type
        self._length = 5

    cdef inline end_message(self):
        # "length-1" to exclude the message type byte
        cdef size_t mlen = self._length - 1

        self._check_readonly()
        if not self._message_mode:
            raise BufferError(
                'end_message can only be called with start_message')
        if self._length < 5:
            raise BufferError('end_message: buffer is too small')

        hton.pack_int32(&self._buf[1], mlen)
        return self

    cdef write_buffer(self, WriteBuffer buf):
        self._check_readonly()

        if not buf._length:
            return

        self._ensure_alloced(buf._length)
        memcpy(self._buf + self._length,
               <void*>buf._buf,
               buf._length)
        self._length += buf._length

    cdef write_byte(self, char b):
        self._check_readonly()

        self._ensure_alloced(1)
        self._buf[self._length] = b
        self._length += 1

    cdef write_bytestring(self, bytes string):
        cdef char* buf
        cdef ssize_t len

        cpython.PyBytes_AsStringAndSize(string, &buf, &len)
        # PyBytes_AsStringAndSize returns a null-terminated buffer,
        # but the null byte is not counted in len. hence the + 1
        self.write_cstr(buf, len + 1)

    cdef write_str(self, str string, str encoding):
        self.write_bytestring(string.encode(encoding))

    cdef write_cstr(self, char *data, ssize_t len):
        self._check_readonly()
        self._ensure_alloced(len)

        memcpy(self._buf + self._length, <void*>data, len)
        self._length += len

    cdef write_int16(self, int16_t i):
        self._check_readonly()
        self._ensure_alloced(2)

        hton.pack_int16(&self._buf[self._length], i)
        self._length += 2

    cdef write_int32(self, int32_t i):
        self._check_readonly()
        self._ensure_alloced(4)

        hton.pack_int32(&self._buf[self._length], i)
        self._length += 4

    cdef write_int64(self, int64_t i):
        self._check_readonly()
        self._ensure_alloced(8)

        hton.pack_int64(&self._buf[self._length], i)
        self._length += 8

    @staticmethod
    cdef WriteBuffer new_message(char type):
        cdef WriteBuffer buf
        buf = WriteBuffer.__new__(WriteBuffer)
        buf.start_message(type)
        return buf

    @staticmethod
    cdef WriteBuffer new():
        cdef WriteBuffer buf
        buf = WriteBuffer.__new__(WriteBuffer)
        return buf


@cython.no_gc_clear
@cython.freelist(_BUFFER_FREELIST_SIZE)
cdef class ReadBuffer:

    def __cinit__(self):
        self._bufs = collections.deque()
        self._bufs_len = 0
        self._buf0 = None
        self._buf0_view = None
        self._pos0 = 0
        self._len0 = 0
        self._length = 0

        self._current_message_type = 0
        self._current_message_len = 0
        self._current_message_len_unread = 0
        self._current_message_ready = 0

    cdef feed_data(self, data):
        cdef int dlen = len(data)

        if dlen == 0:
            # EOF?
            return

        self._bufs.append(data)
        self._length += dlen

        if self._bufs_len == 0:
            # First buffer
            self._len0 = dlen
            self._buf0 = data
            self._buf0_view = memoryview(self._buf0)

        self._bufs_len += 1

    cdef inline _ensure_first_buf(self):
        if self._len0 == 0:
            raise BufferError('empty first buffer')

        if self._pos0 == self._len0:
            # The first buffer is fully read, discard it
            self._bufs.popleft()
            self._bufs_len -= 1

            # Shouldn't fail, since we've checked that `_length >= 1`
            # in the beginning of this method.
            self._buf0 = self._bufs[0]
            self._buf0_view = memoryview(self._buf0)

            self._pos0 = 0
            self._len0 = len(self._buf0)

            IF DEBUG:
                if self._len0 < 1:
                    raise RuntimeError(
                        'debug: second buffer of ReadBuffer is empty')

    cdef inline read_byte(self):
        if self._length < 1:
            raise BufferError('not enough data to read one byte')

        IF DEBUG:
            if not self._buf0:
                raise RuntimeError(
                    'debug: first buffer of ReadBuffer is empty')

        self._ensure_first_buf()

        if self._current_message_ready:
            self._current_message_len_unread -= 1
            if self._current_message_len_unread < 0:
                raise BufferError('buffer overread')

        byte = self._buf0[self._pos0]
        self._pos0 += 1
        self._length -= 1
        return byte

    cdef inline char* _try_read_bytes(self, int nbytes):
        # Important: caller must call _ensure_first_buf() prior
        # to calling try_read_bytes, and must not overread

        cdef:
            char * result

        if nbytes > self._length:
            return NULL

        if self._current_message_ready:
            if self._current_message_len_unread < nbytes:
                return NULL

        if self._pos0 + nbytes <= self._len0:
            result = cpython.PyBytes_AsString(self._buf0)
            result += self._pos0
            self._pos0 += nbytes
            self._length -= nbytes
            if self._current_message_ready:
                self._current_message_len_unread -= nbytes
            return result
        else:
            return NULL

    cdef inline read(self, int nbytes):
        cdef:
            object result
            int nread

        if nbytes > self._length:
            raise BufferError(
                'not enough data to read {} bytes'.format(nbytes))

        self._ensure_first_buf()

        if self._current_message_ready:
            self._current_message_len_unread -= nbytes
            if self._current_message_len_unread < 0:
                raise BufferError('buffer overread')

        if self._pos0 + nbytes <= self._len0:
            result = self._buf0_view[self._pos0 : self._pos0 + nbytes]
            self._pos0 += nbytes
            self._length -= nbytes
            return result

        result = bytearray()
        while True:
            if self._pos0 + nbytes > self._len0:
                result.extend(self._buf0_view[self._pos0:])
                nread = self._len0 - self._pos0
                self._pos0 = self._len0
                self._length -= nread
                nbytes -= nread
                self._ensure_first_buf()

            else:
                result.extend(self._buf0_view[self._pos0:self._pos0 + nbytes])
                self._pos0 += nbytes
                self._length -= nbytes
                return memoryview(result)

    cdef inline read_int32(self):
        cdef:
            object buf
            char *cbuf
            Py_buffer *pybuf

        self._ensure_first_buf()
        cbuf = self._try_read_bytes(4)
        if cbuf != NULL:
            return hton.unpack_int32(cbuf)
        else:
            buf = self.read(4)
            IF DEBUG:
                if not PyMemoryView_Check(buf):
                    raise RuntimeError(
                        'ReadBuffer.read returned non-memoryview')
            pybuf = PyMemoryView_GET_BUFFER(buf)
            return hton.unpack_int32(<char*>(pybuf.buf))

    cdef inline read_int16(self):
        cdef:
            object buf
            char *cbuf
            Py_buffer *pybuf

        self._ensure_first_buf()
        cbuf = self._try_read_bytes(2)
        if cbuf != NULL:
            return hton.unpack_int16(cbuf)
        else:
            buf = self.read(2)
            IF DEBUG:
                if not PyMemoryView_Check(buf):
                    raise RuntimeError(
                        'ReadBuffer.read returned non-memoryview')
            pybuf = PyMemoryView_GET_BUFFER(buf)
            return hton.unpack_int16(<char*>(pybuf.buf))

    cdef inline read_cstr(self):
        if not self._current_message_ready:
            raise BufferError(
                'read_cstr only works when the message guaranteed '
                'to be in the buffer')

        cdef:
            int pos
            int nread
            bytes result = b''

        self._ensure_first_buf()
        while True:
            pos = self._buf0.find(b'\x00', self._pos0)
            if pos >= 0:
                result += self._buf0_view[self._pos0 : pos]
                nread = pos - self._pos0 + 1
                self._pos0 = pos + 1
                self._length -= nread

                self._current_message_len_unread -= nread
                if self._current_message_len_unread < 0:
                    raise BufferError('read_cstr: buffer overread')

                return result

            else:
                result += self._buf0_view[self._pos0:]
                nread = self._len0 - self._pos0
                self._pos0 = self._len0
                self._length -= nread

                self._current_message_len_unread -= nread
                if self._current_message_len_unread < 0:
                    raise BufferError('read_cstr: buffer overread')

                self._ensure_first_buf()

    cdef has_message(self):
        if self._current_message_ready:
            return True

        if self._current_message_type == 0:
            if self._length < 1:
                return False
            self._current_message_type = self.read_byte()

        if self._current_message_len == 0:
            if self._length < 4:
                return False
            self._current_message_len = self.read_int32()
            self._current_message_len_unread = self._current_message_len - 4

        if self._length < self._current_message_len_unread:
            return False

        self._current_message_ready = 1
        return True

    cdef consume_message(self):
        if not self._current_message_ready:
            raise BufferError('no message to consume')
        return self.read(self._current_message_len_unread)

    cdef discard_message(self):
        if not self._current_message_ready:
            raise BufferError('no message to discard')

        if self._current_message_len_unread:
            discarded = self.consume_message()
            IF DEBUG:
                print('!!! discarding message {!r} unread data: {!r}'.format(
                    chr(self._current_message_type), bytes(discarded)))

        self._current_message_type = 0
        self._current_message_len = 0
        self._current_message_ready = 0
        self._current_message_len_unread = 0

    cdef get_message_type(self):
        return self._current_message_type

    cdef get_message_length(self):
        return self._current_message_len

    @staticmethod
    cdef ReadBuffer new_message_parser(object data):
        cdef ReadBuffer buf

        buf = ReadBuffer.__new__(ReadBuffer)
        buf.feed_data(bytes(data))

        buf._current_message_ready = 1
        buf._current_message_len_unread = buf._len0

        return buf
