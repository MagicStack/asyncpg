# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import os
import unittest

import asyncpg
import asyncpg.serverversion

from asyncpg import _testbase as tb


class TestEnvironment(tb.ConnectedTestCase):
    @unittest.skipIf(not os.environ.get('PGVERSION'),
                     "environ[PGVERSION] is not set")
    async def test_environment_server_version(self):
        pgver = os.environ.get('PGVERSION')
        env_ver = asyncpg.serverversion.split_server_version_string(pgver)
        srv_ver = self.con.get_server_version()

        self.assertEqual(
            env_ver[:2], srv_ver[:2],
            'Expecting PostgreSQL version {pgver}, got {maj}.{min}.'.format(
                pgver=pgver, maj=srv_ver.major, min=srv_ver.minor)
        )

    @unittest.skipIf(not os.environ.get('ASYNCPG_VERSION'),
                     "environ[ASYNCPG_VERSION] is not set")
    @unittest.skipIf("dev" in asyncpg.__version__,
                     "development version with git commit data")
    async def test_environment_asyncpg_version(self):
        apgver = os.environ.get('ASYNCPG_VERSION')
        self.assertEqual(
            asyncpg.__version__, apgver,
            'Expecting asyncpg version {}, got {}.'.format(
                apgver, asyncpg.__version__)
        )
