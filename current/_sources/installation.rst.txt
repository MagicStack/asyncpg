.. _asyncpg-installation:


Installation
============

**asyncpg** has no external dependencies and the recommended way to
install it is to use **pip**:

.. code-block:: bash

    $ pip install asyncpg


.. note::

   It is recommended to use **pip** version **8.1** or later to take
   advantage of the precompiled wheel packages.  Older versions of pip
   will ignore the wheel packages and install asyncpg from the source
   package.  In that case a working C compiler is required.


Building from source
--------------------

If you want to build **asyncpg** from a Git checkout you will need:

  * To have cloned the repo with `--recurse-submodules`.
  * A working C compiler.
  * CPython header files.  These can usually be obtained by installing
    the relevant Python development package: **python3-dev** on Debian/Ubuntu,
    **python3-devel** on RHEL/Fedora.

Once the above requirements are satisfied, run the following command
in the root of the source checkout:

.. code-block:: bash

    $ pip install -e .

A debug build containing more runtime checks can be created by setting
the ``ASYNCPG_DEBUG`` environment variable when building:

.. code-block:: bash

    $ env ASYNCPG_DEBUG=1 pip install -e .


Running tests
-------------


If you want to run tests you must have PostgreSQL installed.

To execute the testsuite run:

.. code-block:: bash

    $ python setup.py test
