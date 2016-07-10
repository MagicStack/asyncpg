from . import compat
from . import introspection


class PreparedStatement:

    __slots__ = ('_connection', '_state', '_query')

    def __init__(self, connection, query, state):
        self._connection = connection
        self._state = state
        self._query = query

    def get_parameters(self):
        self.__check_open()
        return self._state._get_parameters()

    def get_attributes(self):
        self.__check_open()
        return self._state._get_attributes()

    def get_aiter(self, *args):
        self.__check_open()
        return PreparedStatementIterator(self, args)

    async def get_list(self, *args):
        self.__check_open()
        protocol = self._connection._protocol
        data = await protocol.execute(self._state, args)
        if data is None:
            data = []
        return data

    async def get_value(self, *args, column=0):
        self.__check_open()
        protocol = self._connection._protocol
        data = await protocol.execute(self._state, args)
        if data is None:
            return None
        return data[0][column]

    async def get_first_row(self, *args):
        self.__check_open()
        protocol = self._connection._protocol
        data = await protocol.execute(self._state, args)
        if data is None:
            return None
        return data[0]

    def __check_open(self):
        if self._state.closed:
            raise RuntimeError('prepared statement is closed')


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
