from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t


cdef extern from "arpa/inet.h":
    uint32_t htonl(uint32_t hostlong)
    uint16_t htons(uint16_t hostshort)
    uint32_t ntohl(uint32_t netlong)
    uint16_t ntohs(uint16_t netshort)


cdef inline void pack_int16(char* buf, int16_t x):
    (<uint16_t*>buf)[0] = htons(<uint16_t>x)


cdef inline int16_t unpack_int16(const char* buf):
    return <int16_t>ntohs((<uint16_t*>buf)[0])


cdef inline void pack_int32(char* buf, int32_t x):
    (<uint32_t*>buf)[0] = htonl(<uint32_t>x)


cdef inline int32_t unpack_int32(const char* buf):
    return <int32_t>ntohl((<uint32_t*>buf)[0])


cdef inline void pack_int64(char* buf, int64_t x):
    (<uint32_t*>buf)[0] = htonl(<uint32_t>(<uint64_t>(x) >> 32))
    (<uint32_t*>&buf[4])[0] = htonl(<uint32_t>(x))


cdef inline int64_t unpack_int64(const char* buf):
    cdef int64_t hh = unpack_int32(buf)
    cdef uint32_t hl = unpack_int32(&buf[4])

    return (hh << 32) | hl
