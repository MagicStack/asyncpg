# cython: language_level=3


from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t

include "consts.pxi"
include "pgtypes.pxi"

include "buffer.pxd"
include "codecs/base.pxd"
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

    STATE_CLOSING = 100
    STATE_CLOSED = 101


cdef class BaseProtocol(CoreProtocol):

    cdef:
        object _loop
        object _address
        tuple  _hash
        ConnectionSettings _settings
        str _last_query
        object _connect_waiter
        object _waiter

        ProtocolState _state

        PreparedStatementState _prepared_stmt

        int _id
        int _N

    cdef inline _create_future(self)
    cdef _gen_id(self, prefix)
    cdef _start_state(self, ProtocolState state)
    cdef _handle_waiter_on_connection_lost(self, cause)
