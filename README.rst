asyncpg-rkt -- A fast PostgreSQL Database Client Library for Python/asyncio that returns numpy arrays
=====================================================================================================

.. image:: https://github.com/athenianco/asyncpg-rkt/workflows/Tests/badge.svg
   :target: https://github.com/athenianco/asyncpg-rkt/actions?query=workflow%3ATests+branch%3Amaster
   :alt: GitHub Actions status
.. image:: https://img.shields.io/pypi/v/asyncpg.svg
   :target: https://pypi.python.org/pypi/asyncpg

**asyncpg-rkt** is a fork of **asyncpg**, a database interface library designed specifically for
PostgreSQL and Python/asyncio.  asyncpg is an efficient, clean implementation
of PostgreSQL server binary protocol for use with Python's ``asyncio``
framework.  You can read more about asyncpg in an introductory
`blog post <http://magic.io/blog/asyncpg-1m-rows-from-postgres-to-python/>`_.

**asyncpg-rkt** extends **asyncpg** as follows:
- Backward compatible with the parent.
- It is possible to set the numpy dtype for the fetched query.
- Such "typed" queries return numpy arrays instead of lists of Record objects.
- We construct numpy arrays directly from the low-level PostgreSQL protocol, without materializing any Python objects.
- Although, we support `object` fields, too.
- The time from receiving the response from PostgreSQL server till `Connection.fetch()` is 10-100x less.
It happens so because we avoid the overhead of dealing with Python objects in the result.
- We return `ravel()`-ed indexes of nulls while writing NaN-s/NaT-s at the corresponding places in the array.

**asyncpg-rkt** ensures the best performance when there are thousands of rows returned and the field types map to numpy.

asyncpg requires Python 3.6 or later and is supported for PostgreSQL
versions 9.5 to 14.  Older PostgreSQL versions or other databases implementing
the PostgreSQL protocol *may* work, but are not being actively tested.


Documentation
-------------

The project documentation can be found
`here <https://athenianco.github.io/asyncpg/current/>`_.


Performance
-----------

In our testing asyncpg is, on average, **3x** faster than psycopg2
(and its asyncio variant -- aiopg).

.. image:: https://raw.githubusercontent.com/athenianco/asyncpg-rkt/master/performance.png
    :target: https://gistpreview.github.io/?b8eac294ac85da177ff82f784ff2cb60

The above results are a geometric mean of benchmarks obtained with PostgreSQL
`client driver benchmarking toolbench <https://github.com/MagicStack/pgbench>`_
in November 2020 (click on the chart to see full details).


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
