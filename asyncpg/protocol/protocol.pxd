# cython: language_level=3


from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t

include "__debug.pxi"
include "consts.pxi"
include "pgtypes.pxi"

include "buffer.pxd"
include "codecs/base.pxd"
include "settings.pxd"
include "coreproto.pxd"
include "prepared_stmt.pxd"


cdef class BaseProtocol(CoreProtocol):

    cdef:
        object loop
        object address
        ConnectionSettings settings
        object waiter
        bint return_extra
        object create_future

        str last_query

        int uid_counter
        bint closing

        PreparedStatementState statement

    cdef _new_waiter(self)

    cdef _on_result__connect(self, object waiter)
    cdef _on_result__prepare(self, object waiter)
    cdef _on_result__bind_and_exec(self, object waiter)
    cdef _on_result__close_stmt_or_portal(self, object waiter)
    cdef _on_result__simple_query(self, object waiter)
    cdef _on_result__bind(self, object waiter)

    cdef _handle_waiter_on_connection_lost(self, cause)

    cdef _dispatch_result(self)
