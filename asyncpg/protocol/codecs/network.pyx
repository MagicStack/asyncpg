# Copyright (C) 2016-present the asyncpg authors and contributors
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


cdef inline uint8_t _ip_max_prefix_len(int32_t family):
    # Maximum number of bits in the network prefix of the specified
    # IP protocol version.
    if family == PGSQL_AF_INET:
        return 32
    else:
        return 128


cdef inline int32_t _ip_addr_len(int32_t family):
    # Length of address in bytes for the specified IP protocol version.
    if family == PGSQL_AF_INET:
        return 4
    else:
        return 16


cdef inline int8_t _ver_to_family(int32_t version):
    if version == 4:
        return PGSQL_AF_INET
    else:
        return PGSQL_AF_INET6


cdef inline _net_encode(WriteBuffer buf, int8_t family, uint32_t bits,
                        int8_t is_cidr, bytes addr):

    cdef:
        char *addrbytes
        ssize_t addrlen

    cpython.PyBytes_AsStringAndSize(addr, &addrbytes, &addrlen)

    buf.write_int32(4 + <int32_t>addrlen)
    buf.write_byte(family)
    buf.write_byte(<int8_t>bits)
    buf.write_byte(is_cidr)
    buf.write_byte(<int8_t>addrlen)
    buf.write_cstr(addrbytes, addrlen)


cdef net_decode(ConnectionSettings settings, FastReadBuffer buf):
    cdef:
        int32_t family = <int32_t>buf.read(1)[0]
        uint8_t bits = <uint8_t>buf.read(1)[0]
        int32_t is_cidr = <int32_t>buf.read(1)[0]
        int32_t addrlen = <int32_t>buf.read(1)[0]
        bytes addr
        uint8_t max_prefix_len = _ip_max_prefix_len(family)

    if family != PGSQL_AF_INET and family != PGSQL_AF_INET6:
        raise ValueError('invalid address family in "{}" value'.format(
            'cidr' if is_cidr else 'inet'
        ))

    max_prefix_len = _ip_max_prefix_len(family)

    if bits > max_prefix_len:
        raise ValueError('invalid network prefix length in "{}" value'.format(
            'cidr' if is_cidr else 'inet'
        ))

    if addrlen != _ip_addr_len(family):
        raise ValueError('invalid address length in "{}" value'.format(
            'cidr' if is_cidr else 'inet'
        ))

    addr = cpython.PyBytes_FromStringAndSize(buf.read(addrlen), addrlen)

    if is_cidr or bits != max_prefix_len:
        return _ipnet(addr).supernet(new_prefix=cpython.PyLong_FromLong(bits))
    else:
        return _ipaddr(addr)


cdef cidr_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        object ipnet
        int8_t family

    ipnet = _ipnet(obj)
    family = _ver_to_family(ipnet.version)
    _net_encode(buf, family, ipnet.prefixlen, 1, ipnet.network_address.packed)


cdef inet_encode(ConnectionSettings settings, WriteBuffer buf, obj):
    cdef:
        object ipaddr
        int8_t family

    try:
        ipaddr = _ipaddr(obj)
    except ValueError:
        # PostgreSQL accepts *both* CIDR and host values
        # for the host datatype.
        cidr_encode(settings, buf, obj)
    else:
        family = _ver_to_family(ipaddr.version)
        _net_encode(buf, family, _ip_max_prefix_len(family), 0, ipaddr.packed)


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

    register_core_codec(MACADDR8OID,
                        <encode_func>&text_encode,
                        <decode_func>&text_decode,
                        PG_FORMAT_TEXT)


init_network_codecs()
