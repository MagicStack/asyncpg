# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import json

from . import cursor
from . import exceptions


class PreparedStatement:
    """A representation of a prepared statement."""

    __slots__ = ('_connection', '_state', '_query', '_last_status')

    def __init__(self, connection, query, state):
        self._connection = connection
        self._state = state
        self._query = query
        state.attach()
        self._last_status = None

    def get_query(self) -> str:
        """Return the text of the query for this prepared statement.

        Example::

            stmt = await connection.prepare('SELECT $1::int')
            assert stmt.get_query() == "SELECT $1::int"
        """
        return self._query

    def get_statusmsg(self) -> str:
        """Return the status of the executed command.

        Example::

            stmt = await connection.prepare('CREATE TABLE mytab (a int)')
            await stmt.fetch()
            assert stmt.get_statusmsg() == "CREATE TABLE"
        """
        if self._last_status is None:
            return self._last_status
        return self._last_status.decode()

    def get_parameters(self):
        """Return a description of statement parameters types.

        :return: A tuple of :class:`asyncpg.types.Type`.

        Example::

            stmt = await connection.prepare('SELECT ($1::int, $2::text)')
            print(stmt.get_parameters())

            # Will print:
            #   (Type(oid=23, name='int4', kind='scalar', schema='pg_catalog'),
            #    Type(oid=25, name='text', kind='scalar', schema='pg_catalog'))
        """
        self.__check_open()
        return self._state._get_parameters()

    def get_attributes(self):
        """Return a description of relation attributes (columns).

        :return: A tuple of :class:`asyncpg.types.Attribute`.

        Example::

            st = await self.con.prepare('''
                SELECT typname, typnamespace FROM pg_type
            ''')
            print(st.get_attributes())

            # Will print:
            #   (Attribute(
            #       name='typname',
            #       type=Type(oid=19, name='name', kind='scalar',
            #                 schema='pg_catalog')),
            #    Attribute(
            #       name='typnamespace',
            #       type=Type(oid=26, name='oid', kind='scalar',
            #                 schema='pg_catalog')))
        """
        self.__check_open()
        return self._state._get_attributes()

    def cursor(self, *args, prefetch=None,
               timeout=None) -> cursor.CursorFactory:
        """Return a *cursor factory* for the prepared statement.

        :param args: Query arguments.
        :param int prefetch: The number of rows the *cursor iterator*
                             will prefetch (defaults to ``50``.)
        :param float timeout: Optional timeout in seconds.

        :return: A :class:`~cursor.CursorFactory` object.
        """
        self.__check_open()
        return cursor.CursorFactory(self._connection, self._query,
                                    self._state, args, prefetch,
                                    timeout)

    async def explain(self, *args, analyze=False):
        """Return the execution plan of the statement.

        :param args: Query arguments.
        :param analyze: If ``True``, the statement will be executed and
                        the run time statitics added to the return value.

        :return: An object representing the execution plan.  This value
                 is actually a deserialized JSON output of the SQL
                 ``EXPLAIN`` command.
        """
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
                data = await self._connection.fetchval(query, *args)
            finally:
                await tr.rollback()
        else:
            data = await self._connection.fetchval(query, *args)

        return json.loads(data)

    async def fetch(self, *args, timeout=None):
        r"""Execute the statement and return a list of :class:`Record` objects.

        :param str query: Query text
        :param args: Query arguments
        :param float timeout: Optional timeout value in seconds.

        :return: A list of :class:`Record` instances.
        """
        self.__check_open()
        protocol = self._connection._protocol
        data, status, _ = await protocol.bind_execute(
            self._state, args, '', 0, True, timeout)
        self._last_status = status
        return data

    async def fetchval(self, *args, column=0, timeout=None):
        """Execute the statement and return a value in the first row.

        :param args: Query arguments.
        :param int column: Numeric index within the record of the value to
                           return (defaults to 0).
        :param float timeout: Optional timeout value in seconds.
                            If not specified, defaults to the value of
                            ``command_timeout`` argument to the ``Connection``
                            instance constructor.

        :return: The value of the specified column of the first record.
        """
        self.__check_open()
        protocol = self._connection._protocol
        data, status, _ = await protocol.bind_execute(
            self._state, args, '', 1, True, timeout)
        self._last_status = status
        if not data:
            return None
        return data[0][column]

    async def fetchrow(self, *args, timeout=None):
        """Execute the statement and return the first row.

        :param str query: Query text
        :param args: Query arguments
        :param float timeout: Optional timeout value in seconds.

        :return: The first row as a :class:`Record` instance.
        """
        self.__check_open()
        protocol = self._connection._protocol
        data, status, _ = await protocol.bind_execute(
            self._state, args, '', 1, True, timeout)
        self._last_status = status
        if not data:
            return None
        return data[0]

    def __check_open(self):
        if self._state.closed:
            raise exceptions.InterfaceError('prepared statement is closed')

    def __del__(self):
        self._state.detach()
        self._connection._maybe_gc_stmt(self._state)
