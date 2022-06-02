asyncpg-🚀 -- A fast PostgreSQL Database Client Library for Python/asyncio that returns numpy arrays
====================================================================================================

.. image:: https://github.com/athenianco/asyncpg-rkt/workflows/Tests/badge.svg
   :target: https://github.com/athenianco/asyncpg-rkt/actions?query=workflow%3ATests+branch%3Amaster
   :alt: GitHub Actions status
.. image:: https://img.shields.io/pypi/v/asyncpg-rkt.svg
   :target: https://pypi.python.org/pypi/asyncpg-rkt

**asyncpg-rkt** is a fork of **asyncpg**, a database interface library designed specifically for
PostgreSQL and Python/asyncio.  asyncpg is an efficient, clean implementation
of PostgreSQL server binary protocol for use with Python's ``asyncio``
framework.  You can read more about asyncpg in an introductory
`blog post <http://magic.io/blog/asyncpg-1m-rows-from-postgres-to-python/>`_.

**asyncpg-rkt** extends **asyncpg** as follows:
- Backward compatible with the origin.
- It is possible to set the numpy dtype for the fetched query.
- Such "typed" queries return numpy arrays instead of lists of Record objects.
- We construct numpy arrays directly from the low-level PostgreSQL protocol, without materializing any Python objects.
- Although, we support `object` fields, too.
- The time from receiving the response from PostgreSQL server until `Connection.fetch()` returns is ~20x less.
This is because we avoid the overhead of dealing with Python objects in the result.
- We return `ravel()`-ed indexes of nulls while writing NaN-s/NaT-s at the corresponding places in the array.
- There is an option to return data by column vs. by row.

**asyncpg-rkt** provides the best performance when there are thousands of rows returned and the field types map to numpy.

Read the blog post with the introduction.

asyncpg-🚀 requires Python 3.7 or later and is supported for PostgreSQL
versions 9.5 to 14.  Older PostgreSQL versions or other databases implementing
the PostgreSQL protocol *may* work, but are not being actively tested.


Documentation
-------------

The project documentation can be found
`here <https://magicstack.github.io/asyncpg/current/>`_.

See below about how to use the fork's special features.

Performance
-----------

In our testing asyncpg is, on average, **3x** faster than psycopg2
(and its asyncio variant -- aiopg).

.. image:: https://raw.githubusercontent.com/athenianco/asyncpg-rkt/master/performance.png
    :target: https://gistpreview.github.io/?b8eac294ac85da177ff82f784ff2cb60

The above results are a geometric mean of benchmarks obtained with PostgreSQL
`client driver benchmarking toolbench <https://github.com/MagicStack/pgbench>`_
in November 2020 (click on the chart to see full details).

Further improvement from writing numpy arrays is ~20x:

.. image:: https://raw.githubusercontent.com/athenianco/asyncpg-rkt/master/benchmark_20220522_142813.svg

.. image:: https://raw.githubusercontent.com/athenianco/asyncpg-rkt/master/benchmark_20220522_143838.svg

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

asyncpg-🚀 is available on PyPI and requires numpy 1.21+.
Use pip to install::

    $ pip install asyncpg-rkt


Basic Usage
-----------

.. code-block:: python

    import asyncio
    import asyncpg
    from asyncpg.rkt import set_query_dtype
    import numpy as np

    async def run():
        conn = await asyncpg.connect(user='user', password='password',
                                     database='database', host='127.0.0.1')
        dtype = np.dtype([
            ("a", int),
            ("b", "datetime64[s]"),
        ])
        array, nulls = await conn.fetch(
            set_query_dtype('SELECT * FROM mytable WHERE id = $1', dtype),
            10,
        )
        await conn.close()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())


License
-------

asyncpg-🚀 is developed and distributed under the Apache 2.0 license, just like the original project.
