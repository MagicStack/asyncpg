# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import collections


__all__ = (
    'Type', 'Attribute', 'Range', 'BitString', 'Point', 'Path', 'Polygon',
    'Box', 'Line', 'LineSegment', 'Circle', 'ServerVersion'
)


Type = collections.namedtuple('Type', ['oid', 'name', 'kind', 'schema'])
Type.__doc__ = 'Database data type.'
Type.oid.__doc__ = 'OID of the type.'
Type.name.__doc__ = 'Type name.  For example "int2".'
Type.kind.__doc__ = \
    'Type kind.  Can be "scalar", "array", "composite" or "range".'
Type.schema.__doc__ = 'Name of the database schema that defines the type.'


Attribute = collections.namedtuple('Attribute', ['name', 'type'])
Attribute.__doc__ = 'Database relation attribute.'
Attribute.name.__doc__ = 'Attribute name.'
Attribute.type.__doc__ = 'Attribute data type :class:`asyncpg.types.Type`.'


ServerVersion = collections.namedtuple(
    'ServerVersion', ['major', 'minor', 'micro', 'releaselevel', 'serial'])
ServerVersion.__doc__ = 'PostgreSQL server version tuple.'


class Range:
    """Immutable representation of PostgreSQL `range` type."""

    __slots__ = '_lower', '_upper', '_lower_inc', '_upper_inc', '_empty'

    def __init__(self, lower=None, upper=None, *,
                 lower_inc=True, upper_inc=False,
                 empty=False):
        self._empty = empty
        if empty:
            self._lower = self._upper = None
            self._lower_inc = self._upper_inc = False
        else:
            self._lower = lower
            self._upper = upper
            self._lower_inc = lower is not None and lower_inc
            self._upper_inc = upper is not None and upper_inc

    @property
    def lower(self):
        return self._lower

    @property
    def lower_inc(self):
        return self._lower_inc

    @property
    def lower_inf(self):
        return self._lower is None and not self._empty

    @property
    def upper(self):
        return self._upper

    @property
    def upper_inc(self):
        return self._upper_inc

    @property
    def upper_inf(self):
        return self._upper is None and not self._empty

    @property
    def isempty(self):
        return self._empty

    def __bool__(self):
        return not self._empty

    def __eq__(self, other):
        if not isinstance(other, Range):
            return NotImplemented

        return (
            self._lower,
            self._upper,
            self._lower_inc,
            self._upper_inc,
            self._empty
        ) == (
            other._lower,
            other._upper,
            other._lower_inc,
            other._upper_inc,
            other._empty
        )

    def __hash__(self, other):
        return hash((
            self._lower,
            self._upper,
            self._lower_inc,
            self._upper_inc,
            self._empty
        ))

    def __repr__(self):
        if self._empty:
            desc = 'empty'
        else:
            if self._lower is None or not self._lower_inc:
                lb = '('
            else:
                lb = '['

            if self._lower is not None:
                lb += repr(self._lower)

            if self._upper is not None:
                ub = repr(self._upper)
            else:
                ub = ''

            if self._upper is None or not self._upper_inc:
                ub += ')'
            else:
                ub += ']'

            desc = '{}, {}'.format(lb, ub)

        return '<Range {}>'.format(desc)

    __str__ = __repr__


class BitString:
    """Immutable representation of PostgreSQL `bit` and `varbit` types."""

    __slots__ = '_bytes', '_bitlength'

    def __init__(self, bitstring=None):
        if not bitstring:
            self._bytes = bytes()
            self._bitlength = 0
        else:
            bytelen = len(bitstring) // 8 + 1
            bytes_ = bytearray(bytelen)
            byte = 0
            byte_pos = 0
            bit_pos = 0

            for i, bit in enumerate(bitstring):
                if bit == ' ':
                    continue
                bit = int(bit)
                if bit != 0 and bit != 1:
                    raise ValueError(
                        'invalid bit value at position {}'.format(i))

                byte |= bit << (8 - bit_pos - 1)
                bit_pos += 1
                if bit_pos == 8:
                    bytes_[byte_pos] = byte
                    byte = 0
                    byte_pos += 1
                    bit_pos = 0

            if bit_pos != 0:
                bytes_[byte_pos] = byte

            bitlen = byte_pos * 8 + bit_pos
            bytelen = byte_pos + (1 if bit_pos else 0)

            self._bytes = bytes(bytes_[:bytelen])
            self._bitlength = bitlen

    @classmethod
    def frombytes(cls, bytes_=None, bitlength=None):
        if bitlength is None and bytes_ is None:
            bytes_ = bytes()
            bitlength = 0

        elif bitlength is None:
            bitlength = len(bytes_) * 8

        else:
            if bytes_ is None:
                bytes_ = bytes(bitlength // 8 + 1)
                bitlength = bitlength
            else:
                bytes_len = len(bytes_) * 8

                if bytes_len == 0 and bitlength != 0:
                    raise ValueError('invalid bit length specified')

                if bytes_len != 0 and bitlength == 0:
                    raise ValueError('invalid bit length specified')

                if bitlength < bytes_len - 8:
                    raise ValueError('invalid bit length specified')

                if bitlength > bytes_len:
                    raise ValueError('invalid bit length specified')

        result = cls()
        result._bytes = bytes_
        result._bitlength = bitlength

        return result

    @property
    def bytes(self):
        return self._bytes

    def as_string(self):
        s = ''

        for i in range(self._bitlength):
            s += str(self._getitem(i))
            if i % 4 == 3:
                s += ' '

        return s.strip()

    def __repr__(self):
        return '<BitString {}>'.format(self.as_string())

    __str__ = __repr__

    def __eq__(self, other):
        if not isinstance(other, BitString):
            return NotImplemented

        return (self._bytes == other._bytes and
                self._bitlength == other._bitlength)

    def __hash__(self):
        return hash((self._bytes, self._bitlength))

    def _getitem(self, i):
        byte = self._bytes[i // 8]
        shift = 8 - i % 8 - 1
        return (byte >> shift) & 0x1

    def __getitem__(self, i):
        if isinstance(i, slice):
            raise NotImplementedError('BitString does not support slices')

        if i >= self._bitlength:
            raise IndexError('index out of range')

        return self._getitem(i)

    def __len__(self):
        return self._bitlength


class Point(tuple):
    """Immutable representation of PostgreSQL `point` type."""

    __slots__ = ()

    def __new__(cls, x, y):
        return super().__new__(cls, (float(x), float(y)))

    def __repr__(self):
        return '{}.{}({})'.format(
            type(self).__module__,
            type(self).__name__,
            tuple.__repr__(self)
        )

    @property
    def x(self):
        return self[0]

    @property
    def y(self):
        return self[1]


class Box(tuple):
    """Immutable representation of PostgreSQL `box` type."""

    __slots__ = ()

    def __new__(cls, high, low):
        return super().__new__(cls, (Point(*high), Point(*low)))

    def __repr__(self):
        return '{}.{}({})'.format(
            type(self).__module__,
            type(self).__name__,
            tuple.__repr__(self)
        )

    @property
    def high(self):
        return self[0]

    @property
    def low(self):
        return self[1]


class Line(tuple):
    """Immutable representation of PostgreSQL `line` type."""

    __slots__ = ()

    def __new__(cls, A, B, C):
        return super().__new__(cls, (A, B, C))

    @property
    def A(self):
        return self[0]

    @property
    def B(self):
        return self[1]

    @property
    def C(self):
        return self[2]


class LineSegment(tuple):
    """Immutable representation of PostgreSQL `lseg` type."""

    __slots__ = ()

    def __new__(cls, p1, p2):
        return super().__new__(cls, (Point(*p1), Point(*p2)))

    def __repr__(self):
        return '{}.{}({})'.format(
            type(self).__module__,
            type(self).__name__,
            tuple.__repr__(self)
        )

    @property
    def p1(self):
        return self[0]

    @property
    def p2(self):
        return self[1]


class Path:
    """Immutable representation of PostgreSQL `path` type."""

    __slots__ = '_is_closed', 'points'

    def __init__(self, *points, is_closed=False):
        self.points = tuple(Point(*p) for p in points)
        self._is_closed = is_closed

    @property
    def is_closed(self):
        return self._is_closed

    def __eq__(self, other):
        if not isinstance(other, Path):
            return NotImplemented

        return (self.points == other.points and
                self._is_closed == other._is_closed)

    def __hash__(self):
        return hash((self.points, self.is_closed))

    def __iter__(self):
        return iter(self.points)

    def __len__(self):
        return len(self.points)

    def __getitem__(self, i):
        return self.points[i]

    def __contains__(self, point):
        return point in self.points


class Polygon(Path):
    """Immutable representation of PostgreSQL `polygon` type."""

    __slots__ = ()

    def __init__(self, *points):
        # polygon is always closed
        super().__init__(*points, is_closed=True)


class Circle(tuple):
    """Immutable representation of PostgreSQL `circle` type."""

    __slots__ = ()

    def __new__(cls, center, radius):
        return super().__new__(cls, (center, radius))

    @property
    def center(self):
        return self[0]

    @property
    def radius(self):
        return self[1]
