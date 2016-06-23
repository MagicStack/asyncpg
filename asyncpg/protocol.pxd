# cython: language_level=3

from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t

include "consts.pxi"
include "pgtypes.pxi"

include "buffer.pxd"
include "codecs/init.pxd"
include "settings.pxd"
include "coreproto.pxd"
include "prepared_stmt.pxd"


cdef enum ProtocolState:
    STATE_NOT_CONNECTED = 0
    STATE_READY = 10

    STATE_PREPARE_BIND = 20
    STATE_PREPARE_DESCRIBE = 21

    STATE_EXECUTE = 30

    STATE_QUERY = 40


cdef class BaseProtocol(CoreProtocol):

    cdef:
        object _loop

        object _connect_waiter
        object _waiter
        object _address
        tuple  _hash
        dict   _type_codecs_cache

        ProtocolState _state

        PreparedStatementState _prepared_stmt

        int _id

    cdef inline _create_future(self)
    cdef _gen_id(self, prefix)
    cdef _start_state(self, ProtocolState state)
    cdef _on_result(self, Result result)
