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

Short answer: no.  asyncpg uses asynchronous execution model
and API, which is fundamentally incompatible with SQLAlchemy.
However, it is possible to use asyncpg and SQLAlchemy Core
with the help of a third-party adapter, such as asyncpgsa_.


Can I use dot-notation with :class:`asyncpg.Record`?  It looks cleaner.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We decided against making :class:`asyncpg.Record` a named tuple
because we want to keep the ``Record`` method namespace separate
from the column namespace.


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
prepared statements.  You have two options:

* if you are using pgbouncer for connection pooling to a single server,
  switch to the :ref:`connection pool <asyncpg-connection-pool>`
  functionality provided by asyncpg, it is a much better option for this
  purpose;

* if you have no option of avoiding the use of pgbouncer, then you need to
  switch pgbouncer's ``pool_mode`` to ``session``.


Why do I get ``PostgresSyntaxError`` when using ``expression IN $1``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``expression IN $1`` is not a valid PostgreSQL syntax.  To check
a value against a sequence use ``expression = any($1::mytype[])``,
where ``mytype`` is the array element type.

.. _asyncpgsa: https://github.com/CanopyTax/asyncpgsa
