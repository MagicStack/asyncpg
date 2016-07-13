import json

from . import compat


class PreparedStatement:

    __slots__ = ('_connection', '_state', '_query')

    def __init__(self, connection, query, state):
        self._connection = connection
        self._state = state
        self._query = query
        state.attach()

    def get_parameters(self):
        self.__check_open()
        return self._state._get_parameters()

    def get_attributes(self):
        self.__check_open()
        return self._state._get_attributes()

    def get_aiter(self, *args):
        self.__check_open()
        return PreparedStatementIterator(self, args)

    async def explain(self, *args, analyze=False):
        query = 'EXPLAIN (FORMAT JSON, VERBOSE'
        if analyze:
            query += ', ANALYZE) '
        else:
            query += ') '
        query += self._state.query

        if analyze:
            # From PostgreSQL docs:
            # Important: Keep in mind that the statement is actually
            # executed when the ANALYZE option is used. Although EXPLAIN
            # will discard any output that a SELECT would return, other
            # side effects of the statement will happen as usual. If you
            # wish to use EXPLAIN ANALYZE on an INSERT, UPDATE, DELETE,
            # CREATE TABLE AS, or EXECUTE statement without letting the
            # command affect your data, use this approach:
            #     BEGIN;
            #     EXPLAIN ANALYZE ...;
            #     ROLLBACK;
            tr = self._connection.transaction()
            await tr.start()
            try:
                data = await self._connection.fetch_value(query, *args)
            finally:
                await tr.rollback()
        else:
            data = await self._connection.fetch_value(query, *args)

        return json.loads(data)

    async def fetch(self, *args):
        self.__check_open()
        protocol = self._connection._protocol
        data = await protocol.execute(self._state, args, 0)
        if data is None:
            data = []
        return data

    async def fetch_value(self, *args, column=0):
        self.__check_open()
        protocol = self._connection._protocol
        data = await protocol.execute(self._state, args, 1)
        if data is None:
            return None
        return data[0][column]

    async def fetch_row(self, *args):
        self.__check_open()
        protocol = self._connection._protocol
        data = await protocol.execute(self._state, args, 1)
        if data is None:
            return None
        return data[0]

    def __check_open(self):
        if self._state.closed:
            raise RuntimeError('prepared statement is closed')

    def __del__(self):
        self._state.detach()
        self._connection._maybe_gc_stmt(self._state)


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
            data = await protocol.execute(self._stmt._state, self._args, 0)
            if data is None:
                data = ()
            self._iter = iter(data)

        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration() from None
