import collections


Type = collections.namedtuple('Type', ['oid', 'name', 'kind', 'schema'])


Attribute = collections.namedtuple('Attribute', ['name', 'type'])


class Range:
    """Representation of PostgreSQL `range` type."""

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
