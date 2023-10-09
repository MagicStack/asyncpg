asyncpg -- A fast PostgreSQL Database Client Library for Python/asyncio
=======================================================================

.. image:: https://github.com/MagicStack/asyncpg/workflows/Tests/badge.svg
   :target: https://github.com/MagicStack/asyncpg/actions?query=workflow%3ATests+branch%3Amaster
   :alt: GitHub Actions status
.. image:: https://img.shields.io/pypi/v/asyncpg.svg
   :target: https://pypi.python.org/pypi/asyncpg

**asyncpg** is a database interface library designed specifically for
PostgreSQL and Python/asyncio.  asyncpg is an efficient, clean implementation
of PostgreSQL server binary protocol for use with Python's ``asyncio``
framework.  You can read more about asyncpg in an introductory
`blog post <http://magic.io/blog/asyncpg-1m-rows-from-postgres-to-python/>`_.

asyncpg requires Python 3.8 or later and is supported for PostgreSQL
versions 9.5 to 16.  Older PostgreSQL versions or other databases implementing
the PostgreSQL protocol *may* work, but are not being actively tested.


Documentation
-------------

The project documentation can be found
`here <https://magicstack.github.io/asyncpg/current/>`_.


Performance
-----------

In our testing asyncpg is, on average, **5x** faster than psycopg3.

.. image:: https://raw.githubusercontent.com/MagicStack/asyncpg/master/performance.png?fddca40ab0
    :target: https://gistpreview.github.io/?0ed296e93523831ea0918d42dd1258c2

The above results are a geometric mean of benchmarks obtained with PostgreSQL
`client driver benchmarking toolbench <https://github.com/MagicStack/pgbench>`_
in June 2023 (click on the chart to see full details).


Features
--------

asyncpg implements PostgreSQL server protocol natively and exposes its
features directly, as opposed to hiding them behind a generic facade
like DB-API.

This enables asyncpg to have easy-to-use support for:

* **prepared statements**
* **scrollable cursors**
* **partial iteration** on query results
* automatic encoding and decoding of composite types, arrays,
  and any combination of those
* straightforward support for custom data types


Installation
------------

asyncpg is available on PyPI and has no dependencies.
Use pip to install::

    $ pip install asyncpg


Basic Usage
-----------

.. code-block:: python

    import asyncio
    import asyncpg

    async def run():
        conn = await asyncpg.connect(user='user', password='password',
                                     database='database', host='127.0.0.1')
        values = await conn.fetch(
            'SELECT * FROM mytable WHERE id = $1',
            10,
        )
        await conn.close()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())


License
-------

asyncpg is developed and distributed under the Apache 2.0 license.
