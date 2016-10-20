# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t


IF UNAME_SYSNAME == "Windows":
    cdef extern from "winsock2.h":
        uint32_t htonl(uint32_t hostlong)
        uint16_t htons(uint16_t hostshort)
        uint32_t ntohl(uint32_t netlong)
        uint16_t ntohs(uint16_t netshort)
ELSE:
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


cdef union _floatconv:
    uint32_t i
    float f


cdef inline int32_t pack_float(char* buf, float f):
    cdef _floatconv v
    v.f = f
    pack_int32(buf, v.i)


cdef inline float unpack_float(const char* buf):
    cdef _floatconv v
    v.i = unpack_int32(buf)
    return v.f


cdef union _doubleconv:
    uint64_t i
    double f


cdef inline int64_t pack_double(char* buf, double f):
    cdef _doubleconv v
    v.f = f
    pack_int64(buf, v.i)


cdef inline double unpack_double(const char* buf):
    cdef _doubleconv v
    v.i = unpack_int64(buf)
    return v.f
