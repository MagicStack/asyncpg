.. _asyncpg-installation:


Installation
============

**asyncpg** has no external dependencies when not using GSSAPI/SSPI
authentication.  The recommended way to install it is to use **pip**:

.. code-block:: bash

    $ pip install asyncpg

If you need GSSAPI/SSPI authentication, the recommended way is to use

.. code-block:: bash

    $ pip install 'asyncpg[gssauth]'

This installs SSPI support on Windows and GSSAPI support on non-Windows
platforms.  SSPI and GSSAPI interoperate as clients and servers: an SSPI
client can authenticate to a GSSAPI server and vice versa.

On Linux installing GSSAPI requires a working C compiler and Kerberos 5
development files.  The latter can be obtained by installing **libkrb5-dev**
package on Debian/Ubuntu or **krb5-devel** on RHEL/Fedora.  (This is needed
because PyPI does not have Linux wheels for **gssapi**. See `here for the
details <https://github.com/pythongssapi/python-gssapi/issues/200#issuecomment-1032934269>`_.)

It is also possible to use GSSAPI on Windows:

  * `pip install gssapi`
  * Install `Kerberos for Windows <https://web.mit.edu/kerberos/dist/>`_.
  * Set the ``gsslib`` parameter or the ``PGGSSLIB`` environment variable to
    `gssapi` when connecting.


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
