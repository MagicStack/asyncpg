# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

"""Tests how asyncpg behaves in non-ideal conditions."""

import asyncio
import os
import platform
import unittest
import sys

from asyncpg import _testbase as tb


@unittest.skipIf(os.environ.get('PGHOST'), 'using remote cluster for testing')
@unittest.skipIf(
    platform.system() == 'Windows' and
    sys.version_info >= (3, 8),
    'not compatible with ProactorEventLoop which is default in Python 3.8')
class TestConnectionLoss(tb.ProxiedClusterTestCase):
    @tb.with_timeout(30.0)
    async def test_connection_close_timeout(self):
        con = await self.connect()
        self.proxy.trigger_connectivity_loss()
        with self.assertRaises(asyncio.TimeoutError):
            await con.close(timeout=0.5)

    @tb.with_timeout(30.0)
    async def test_pool_release_timeout(self):
        pool = await self.create_pool(
            database='postgres', min_size=2, max_size=2)
        try:
            with self.assertRaises(asyncio.TimeoutError):
                async with pool.acquire(timeout=0.5):
                    self.proxy.trigger_connectivity_loss()
        finally:
            self.proxy.restore_connectivity()
            pool.terminate()

    @tb.with_timeout(30.0)
    async def test_pool_handles_abrupt_connection_loss(self):
        pool_size = 3
        query_runtime = 0.5
        pool_timeout = cmd_timeout = 1.0
        concurrency = 9
        pool_concurrency = (concurrency - 1) // pool_size + 1

        # Worst expected runtime + 20% to account for other latencies.
        worst_runtime = (pool_timeout + cmd_timeout) * pool_concurrency * 1.2

        async def worker(pool):
            async with pool.acquire(timeout=pool_timeout) as con:
                await con.fetch('SELECT pg_sleep($1)', query_runtime)

        def kill_connectivity():
            self.proxy.trigger_connectivity_loss()

        new_pool = self.create_pool(
            database='postgres', min_size=pool_size, max_size=pool_size,
            timeout=cmd_timeout, command_timeout=cmd_timeout)

        with self.assertRunUnder(worst_runtime):
            pool = await new_pool
            try:
                workers = [worker(pool) for _ in range(concurrency)]
                self.loop.call_later(1, kill_connectivity)
                await asyncio.gather(
                    *workers, return_exceptions=True)
            finally:
                pool.terminate()
