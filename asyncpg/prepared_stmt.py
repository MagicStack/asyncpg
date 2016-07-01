from . import compat


class PreparedStatement:

    __slots__ = ('_connection', '_state')

    def __init__(self, connection, state):
        self._connection = connection
        self._state = state

    def get_parameters(self):
        return self._state._get_parameters()

    def get_attributes(self):
        return self._state._get_attributes()

    def __call__(self, *args):
        return PreparedStatementIterator(self, args)

    async def get_list(self, *args):
        protocol = self._connection._protocol
        data = await protocol.execute(self._state, args)
        if data is None:
            data = []
        return data

    async def get_value(self, *args, column=0):
        protocol = self._connection._protocol
        data = await protocol.execute(self._state, args)
        if data is None:
            return None
        return data[0][column]

    async def get_first_row(self, *args):
        protocol = self._connection._protocol
        data = await protocol.execute(self._state, args)
        if data is None:
            return None
        return data[0]


class PreparedStatementIterator:

    __slots__ = ('_stmt', '_args', '_iter')

    def __init__(self, stmt, args):
        self._stmt = stmt
        self._args = args
        self._iter = None

    @compat.aiter_compat
    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._iter is None:
            protocol = self._stmt._connection._protocol
            data = await protocol.execute(self._stmt._state, self._args)
            if data is None:
                data = ()
            self._iter = iter(data)

        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration() from None
