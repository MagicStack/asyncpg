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

   Establish a connection to a :term:`PostgreSQL` server and return a new
   :class:`Connection` object.

   :param dsn: Connection arguments specified using as a single string in the
               following format:
               ``postgres://user:pass@host:port/database?option=value``

   :param host: database host address or a path to the directory containing
                database server UNIX socket (defaults to the default UNIX
                socket, or the value of the ``PGHOST`` environment variable,
                if set).

   :param port: connection port number (defaults to 5432, or the value of
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

   :returns: :class:`Connection` instance.


.. autoclass:: asyncpg.connection.Connection
   :members:


.. autoclass:: asyncpg.prepared_stmt.PreparedStatement
  :members:
