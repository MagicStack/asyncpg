# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import ipaddress


# defined in postgresql/src/include/inet.h
#
DEF PGSQL_AF_INET = 2  # AF_INET
DEF PGSQL_AF_INET6 = 3  # AF_INET + 1


_ipaddr = ipaddress.ip_address
_ipnet = ipaddress.ip_network


cdef inline _net_encode(WriteBuffer buf, int32_t version, uint8_t bits,
                        int8_t is_cidr, bytes addr):

    cdef:
        char *addrbytes
        ssize_t addrlen
        int8_t family

    family = PGSQL_AF_INET if version == 4 else PGSQL_AF_INET6
    cpython.PyBytes_AsStringAndSize(addr, &addrbytes, &addrlen)

    buf.write_int32(4 + addrlen)
    buf.write_byte(family)
    buf.write_byte(bits)
    buf.write_byte(is_cidr)
    buf.write_byte(<int8_t>addrlen)
    buf.write_cstr(addrbytes, addrlen)


cdef net_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int32_t family = <int32_t>buf.read(1)[0]
        uint8_t bits = <uint8_t>buf.read(1)[0]
        uint32_t is_cidr = <uint32_t>buf.read(1)[0]
        uint32_t addrlen = <uint32_t>buf.read(1)[0]
        bytes addr

    if family != PGSQL_AF_INET and family != PGSQL_AF_INET6:
        raise ValueError('invalid address family in "{}" value'.format(
            'cidr' if is_cidr else 'inet'
        ))

    if bits > (32 if family == PGSQL_AF_INET else 128):
        raise ValueError('invalid bits in "{}" value'.format(
            'cidr' if is_cidr else 'inet'
        ))

    if addrlen != (4 if family == PGSQL_AF_INET else 16):
        raise ValueError('invalid length in "{}" value'.format(
            'cidr' if is_cidr else 'inet'
        ))

    addr = cpython.PyBytes_FromStringAndSize(buf.read(addrlen), addrlen)

    if is_cidr or bits > 0:
        return _ipnet(addr).supernet(new_prefix=cpython.PyLong_FromLong(bits))
    else:
        return _ipaddr(addr)


cdef cidr_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        object ipnet

    ipnet = _ipnet(obj)
    _net_encode(buf, ipnet.version, ipnet.prefixlen, 1,
                ipnet.network_address.packed)


cdef inet_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        object ipaddr

    try:
        ipaddr = _ipaddr(obj)
    except ValueError:
        # PostgreSQL accepts *both* CIDR and host values
        # for the host datatype.
        cidr_encode(settings, buf, obj)
    else:
        _net_encode(buf, ipaddr.version, 0, 0, ipaddr.packed)


cdef init_network_codecs():
    register_core_codec(CIDROID,
                        <encode_func>&cidr_encode,
                        <decode_func>&net_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(INETOID,
                        <encode_func>&inet_encode,
                        <decode_func>&net_decode,
                        PG_FORMAT_BINARY)

    register_core_codec(MACADDROID,
                        <encode_func>&text_encode,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)


init_network_codecs()
