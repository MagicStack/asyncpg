.. _asyncpg-api-reference:

=============
API Reference
=============

.. module:: asyncpg
    :synopsis: A fast PostgreSQL Database Client Library for Python/asyncio

.. currentmodule:: asyncpg


.. _asyncpg-api-connection:

Connection
==========

.. coroutinefunction:: connect(dsn=None, *, host=None, port=None, \
                        user=None, password=None, \
                        database=None, loop=None, timeout=60, \
                        statement_cache_size=100, \
                        command_timeout=None, \
                        **opts)

   Establish a connection to a PostgreSQL server and return a new
   :class:`~asyncpg.connection.Connection` object.

   :param dsn: Connection arguments specified using as a single string in the
               following format:
               ``postgres://user:pass@host:port/database?option=value``

   :param host: database host address or a path to the directory containing
                database server UNIX socket (defaults to the default UNIX
                socket, or the value of the ``PGHOST`` environment variable,
                if set).

   :param port: connection port number (defaults to ``5432``, or the value of
                the ``PGPORT`` environment variable, if set)

   :param user: the name of the database role used for authentication
                (defaults to the name of the effective user of the process
                making the connection, or the value of ``PGUSER`` environment
                variable, if set)

   :param password: password used for authentication

   :param loop: An asyncio event loop instance.  If ``None``, the default
                event loop will be used.

   :param float timeout: connection timeout (in seconds, defaults to 60
                         seconds).

   :param float statement_timeout: the default timeout for operations on
                         this connection (the default is no timeout).

   :param int statement_cache_size: the size of prepared statement LRU cache
                         (defaults to 100).

   :returns: :class:`~asyncpg.connection.Connection` instance.


.. autoclass:: asyncpg.connection.Connection()
   :members:


.. _asyncpg-api-prepared-stmt:

Prepared Statements
===================

Prepared statements are a PostgreSQL feature that can be used to optimize the
performance of queries that are executed more than once.  When a query
is *prepared* by a call to :meth:`Connection.prepare`, the server parses,
analyzes and compiles the query allowing to reuse that work once there is
a need to run the same query again.

.. code-block:: pycon

   >>> import asyncpg, asyncio
   >>> loop = asyncio.get_event_loop()
   >>> async def run():
   ...     conn = await asyncpg.connect()
   ...     stmt = await conn.prepare('''SELECT 2 ^ $1''')
   ...     print(await stmt.fetchval(10))
   ...     print(await stmt.fetchval(20))
   ...
   >>> loop.run_until_complete(run())
   1024.0
   1048576.0

.. note::

   asyncpg automatically maintains a small LRU cache for queries executed
   during calls to the :meth:`~Connection.fetch`, :meth:`~Connection.fetchrow`,
   or :meth:`~Connection.fetchval` methods.


.. autoclass:: asyncpg.prepared_stmt.PreparedStatement()
   :members:


.. _asyncpg-api-transaction:

Transactions
============

The most common way to use transactions is through an ``async with`` statement:

.. code-block:: python

   with connection.transaction():
       connection.execute("INSERT INTO mytable VALUES(1, 2, 3)")


asyncpg supports nested transactions (a nested transaction context will create
a `savepoint`_.)

.. _savepoint: https://www.postgresql.org/docs/current/static/sql-savepoint.html


.. autoclass:: asyncpg.transaction.Transaction()
   :members:

   .. describe:: async with c:

      start and commit/rollback the transaction or savepoint block
      automatically when entering and exiting the code inside the
      context manager block.


.. _asyncpg-api-cursor:

Cursors
=======

Cursors are useful when there is a need to iterate over the results of
a large query without fetching all rows at once.  The cursor interface
provided by asyncpg supports *asynchronous iteration* via the ``async for``
statement, and also a way to read row chunks and skip forward over the
result set.

.. note::

   Cursors created by a call to :meth:`PreparedStatement.cursor()`  are
   *non-scrollable*: they can only be read forwards.  To create a scrollable
   cursor, use the ``DECLARE ... SCROLL CURSOR`` SQL statement directly.

.. warning::

   Cursors created by a call to :meth:`PreparedStatement.cursor()`
   cannot be used outside of a transaction.  Any such attempt will result
   in :exc:`~asyncpg.exceptions.InterfaceError`.

   To create a cursor usable outside of a transaction, use the
   ``DECLARE ... CURSOR WITH HOLD`` SQL statement directly.


.. autoclass:: asyncpg.cursor.CursorInterface()
   :members:

   .. describe:: async for row in c

      Execute the statement and iterate over the results asynchronously.

   .. describe:: await c

      Execute the statement and return an instance of
      :class:`~asyncpg.cursor.Cursor` which can be used to navigate over and
      fetch subsets of the query results.


.. autoclass:: asyncpg.cursor.Cursor()
   :members:


.. _asyncpg-api-record:

Record Objects
==============

Each row (or composite type value) returned by calls to ``fetch*`` methods
is represented by an instance of the ``Record`` object.  ``Record`` objects
are similar to instances of ``collections.namedtuple`` and allow addressing
of values either by a numeric index or by a field name:

.. code-block:: pycon

    >>> import asyncpg
    >>> import asyncio
    >>> loop = asyncio.get_event_loop()
    >>> conn = loop.run_until_complete(asyncpg.connect())
    >>> loop.run_until_complete(conn.fetchrow('''
    ...     SELECT oid, rolname, rolsuper FROM pg_roles WHERE rolname = user'''))
    <Record oid=16388 rolname='elvis' rolsuper=True>


.. class:: Record()

   A read-only representation of PostgreSQL row.

   .. describe:: len(r)

      Return the number of fields in record *r*.

   .. describe:: r[field]

      Return the field of *r* with field name *field*.

   .. describe:: iter(r)

      Return an iterator over the *values* of the record *r*.

   .. method:: values()

      Return an iterator over the record values.

   .. method:: keys()

      Return an iterator over the record field names.

   .. method:: items()

      Return an iterator over ``(field, value)`` pairs.
