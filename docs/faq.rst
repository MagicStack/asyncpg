.. _asyncpg-faq:


Frequently Asked Questions
==========================

Does asyncpg support DB-API?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No.  DB-API is a synchronous API, while asyncpg is based
around an asynchronous I/O model.  Thus, full drop-in compatibility
with DB-API is not possible and we decided to design asyncpg API
in a way that is better aligned with PostgreSQL architecture and
terminology.  We will release a synchronous DB-API-compatible version
of asyncpg at some point in the future.


Can I use asyncpg with SQLAlchemy ORM?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes.  SQLAlchemy version 1.4 and later supports the asyncpg dialect natively.
Please refer to its documentation for details.  Older SQLAlchemy versions
may be used in tandem with a third-party adapter such as
asyncpgsa_ or databases_.


Can I use dot-notation with :class:`asyncpg.Record`?  It looks cleaner.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We decided against making :class:`asyncpg.Record` a named tuple
because we want to keep the ``Record`` method namespace separate
from the column namespace.  That said, you can provide a custom ``Record``
class that implements dot-notation via the ``record_class`` argument to
:func:`connect() <asyncpg.connection.connect>` or any of the Record-returning
methods.


Why can't I use a :ref:`cursor <asyncpg-api-cursor>` outside of a transaction?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cursors created by a call to
:meth:`Connection.cursor() <asyncpg.connection.Connection.cursor>` or
:meth:`PreparedStatement.cursor() \
<asyncpg.prepared_stmt.PreparedStatement.cursor>`
cannot be used outside of a transaction.  Any such attempt will result in
``InterfaceError``.
To create a cursor usable outside of a transaction, use the
``DECLARE ... CURSOR WITH HOLD`` SQL statement directly.


.. _asyncpg-prepared-stmt-errors:

Why am I getting prepared statement errors?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are getting intermittent ``prepared statement "__asyncpg_stmt_xx__"
does not exist`` or ``prepared statement “__asyncpg_stmt_xx__”
already exists`` errors, you are most likely not connecting to the
PostgreSQL server directly, but via
`pgbouncer <https://pgbouncer.github.io/>`_.  pgbouncer, when
in the ``"transaction"`` or ``"statement"`` pooling mode, does not support
prepared statements.  You have several options:

* if you are using pgbouncer only to reduce the cost of new connections
  (as opposed to using pgbouncer for connection pooling from
  a large number of clients in the interest of better scalability),
  switch to the :ref:`connection pool <asyncpg-connection-pool>`
  functionality provided by asyncpg, it is a much better option for this
  purpose;

* disable automatic use of prepared statements by passing
  ``statement_cache_size=0``
  to :func:`asyncpg.connect() <asyncpg.connection.connect>` and
  :func:`asyncpg.create_pool() <asyncpg.pool.create_pool>`
  (and, obviously, avoid the use of
  :meth:`Connection.prepare() <asyncpg.connection.Connection.prepare>`);

* switch pgbouncer's ``pool_mode`` to ``session``.


Why do I get ``PostgresSyntaxError`` when using ``expression IN $1``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``expression IN $1`` is not a valid PostgreSQL syntax.  To check
a value against a sequence use ``expression = any($1::mytype[])``,
where ``mytype`` is the array element type.

.. _asyncpgsa: https://github.com/CanopyTax/asyncpgsa
.. _databases: https://github.com/encode/databases
