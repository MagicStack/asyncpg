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

  * A working C compiler.
  * CPython header files.  These can usually be obtained by installing
    the relevant Python development package: **python3-dev** on Debian/Ubuntu,
    **python3-devel** on RHEL/Fedora.
  * Cython version 0.24 or later.  The easiest way to install it to use
    virtualenv and pip, however a system package should also suffice.
  * GNU make

Once the above requirements are satisfied, run:

.. code-block:: bash

    $ make

At this point you can run the usual ``setup.py`` commands or
``pip install -e .`` to install the newly built version.

.. note::

   A debug build can be created by running ``make debug``.


Running tests
-------------

To execute the testsuite simply run:

.. code-block:: bash

    $ make test
