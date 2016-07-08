from . import compat
from . import introspection


class PreparedStatement:

    __slots__ = ('_connection', '_state', '_query', '_managed', '_closed')

    def __init__(self, connection, query):
        self._connection = connection
        self._state = None
        self._query = query
        self._managed = False
        self._closed = False

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

    async def free(self):
        if self._closed:
            return

        self._closed = True

        if self._state is None:
            return

        self._state = None

    # Private methods:

    def __check_open(self):
        if self._closed:
            raise RuntimeError(
                'cannot perform an operation on closed prepared statement')
        if self._state is None:
            raise RuntimeError('prepared statement is not initialized')

    async def __prepare(self):
        if self._closed:
            raise RuntimeError(
                'cannot initialize closed prepared statement')
        if self._state is not None:
            raise RuntimeError('prepared statement is already initialized')

        con = self._connection
        protocol = con._protocol

        state = await protocol.prepare(None, self._query)

        ready = state._init_types()
        if ready is not True:
            if con._types_stmt is None:
                con._types_stmt = await con.prepare(
                    introspection.INTRO_LOOKUP_TYPES)

            types = await con._types_stmt.get_list(list(ready))
            protocol.get_settings().register_data_types(types)

        self._state = state
        return self

    def __await__(self):
        return self.__prepare().__await__()

    async def __aenter__(self):
        if self._managed:
            raise RuntimeError(
                'nested "async with" is not allowed for prepared statements')
        self._managed = True

        if self._state is None:
            await self.__prepare()

        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._managed = False
        await self.free()


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
