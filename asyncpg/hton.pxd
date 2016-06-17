from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t


cdef extern from "arpa/inet.h":
    uint32_t htonl(uint32_t hostlong)
    uint16_t htons(uint16_t hostshort)
    uint32_t ntohl(uint32_t netlong)
    uint16_t ntohs(uint16_t netshort)


cdef inline void pack_int16(char* buf, uint16_t x):
    (<uint16_t*>buf)[0] = htons(x)


cdef inline int16_t unpack_int16(const char* buf):
    cdef int16_t nx
    (&nx)[0] = (<int16_t*>buf)[0]
    return ntohs(nx)


cdef inline void pack_int32(char* buf, uint32_t x):
    (<uint32_t*>buf)[0] = htonl(x)


cdef inline int32_t unpack_int32(const char* buf):
    cdef int32_t nx
    (&nx)[0] = (<int32_t*>buf)[0]
    return ntohl(nx)


cdef inline void pack_int64(char* buf, uint64_t x):
    (<uint32_t*>buf)[0] = htonl(x)
    (<uint32_t*>&buf[4])[0] = htonl(<uint32_t>(x >> 32))


cdef inline int64_t unpack_int64(const char* buf):
    cdef int32_t hl = unpack_int32(buf)
    cdef int32_t hh = unpack_int32(&buf[4])

    return (<int64_t>(hh) << 32) | <uint32_t>(hl)
