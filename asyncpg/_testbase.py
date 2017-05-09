# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import atexit
import contextlib
import functools
import inspect
import logging
import os
import re
import time
import unittest


from asyncpg import cluster as pg_cluster
from asyncpg import connection as pg_connection
from asyncpg import pool as pg_pool


@contextlib.contextmanager
def silence_asyncio_long_exec_warning():
    def flt(log_record):
        msg = log_record.getMessage()
        return not msg.startswith('Executing ')

    logger = logging.getLogger('asyncio')
    logger.addFilter(flt)
    try:
        yield
    finally:
        logger.removeFilter(flt)


class TestCaseMeta(type(unittest.TestCase)):

    @staticmethod
    def _iter_methods(bases, ns):
        for base in bases:
            for methname in dir(base):
                if not methname.startswith('test_'):
                    continue

                meth = getattr(base, methname)
                if not inspect.iscoroutinefunction(meth):
                    continue

                yield methname, meth

        for methname, meth in ns.items():
            if not methname.startswith('test_'):
                continue

            if not inspect.iscoroutinefunction(meth):
                continue

            yield methname, meth

    def __new__(mcls, name, bases, ns):
        for methname, meth in mcls._iter_methods(bases, ns):
            @functools.wraps(meth)
            def wrapper(self, *args, __meth__=meth, **kwargs):
                self.loop.run_until_complete(__meth__(self, *args, **kwargs))
            ns[methname] = wrapper

        return super().__new__(mcls, name, bases, ns)


class TestCase(unittest.TestCase, metaclass=TestCaseMeta):

    @classmethod
    def setUpClass(cls):
        if os.environ.get('USE_UVLOOP'):
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        cls.loop = loop

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()
        asyncio.set_event_loop(None)

    @contextlib.contextmanager
    def assertRunUnder(self, delta):
        st = time.monotonic()
        try:
            yield
        finally:
            if time.monotonic() - st > delta:
                raise AssertionError(
                    'running block took longer than {}'.format(delta))

    @contextlib.contextmanager
    def assertLoopErrorHandlerCalled(self, msg_re: str):
        contexts = []

        def handler(loop, ctx):
            contexts.append(ctx)

        old_handler = self.loop.get_exception_handler()
        self.loop.set_exception_handler(handler)
        try:
            yield

            for ctx in contexts:
                msg = ctx.get('message')
                if msg and re.search(msg_re, msg):
                    return

            raise AssertionError(
                'no message matching {!r} was logged with '
                'loop.call_exception_handler()'.format(msg_re))

        finally:
            self.loop.set_exception_handler(old_handler)


_default_cluster = None


def _start_cluster(ClusterCls, cluster_kwargs, server_settings):
    cluster = ClusterCls(**cluster_kwargs)
    cluster.init()
    cluster.trust_local_connections()
    cluster.start(port='dynamic', server_settings=server_settings)
    atexit.register(_shutdown_cluster, cluster)
    return cluster


def _start_default_cluster(server_settings={}):
    global _default_cluster

    if _default_cluster is None:
        pg_host = os.environ.get('PGHOST')
        if pg_host:
            # Using existing cluster, assuming it is initialized and running
            _default_cluster = pg_cluster.RunningCluster()
        else:
            _default_cluster = _start_cluster(
                pg_cluster.TempCluster, {}, server_settings)

    return _default_cluster


def _shutdown_cluster(cluster):
    cluster.stop()
    cluster.destroy()


def create_pool(dsn=None, *,
                min_size=10,
                max_size=10,
                max_queries=50000,
                max_inactive_connection_lifetime=60.0,
                setup=None,
                init=None,
                loop=None,
                pool_class=pg_pool.Pool,
                connection_class=pg_connection.Connection,
                **connect_kwargs):
    return pool_class(
        dsn,
        min_size=min_size, max_size=max_size,
        max_queries=max_queries, loop=loop, setup=setup, init=init,
        max_inactive_connection_lifetime=max_inactive_connection_lifetime,
        connection_class=connection_class,
        **connect_kwargs)


class ClusterTestCase(TestCase):
    @classmethod
    def get_server_settings(cls):
        return {
            'log_connections': 'on'
        }

    @classmethod
    def setup_cluster(cls):
        cls.cluster = _start_default_cluster(cls.get_server_settings())

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.setup_cluster()

    def create_pool(self, pool_class=pg_pool.Pool, **kwargs):
        conn_spec = self.cluster.get_connection_spec()
        conn_spec.update(kwargs)
        return create_pool(loop=self.loop, pool_class=pool_class, **conn_spec)

    @classmethod
    def start_cluster(cls, ClusterCls, *,
                      cluster_kwargs={}, server_settings={}):
        return _start_cluster(ClusterCls, cluster_kwargs, server_settings)


def with_connection_options(**options):
    if not options:
        raise ValueError('no connection options were specified')

    def wrap(func):
        func.__connect_options__ = options
        return func

    return wrap


class ConnectedTestCase(ClusterTestCase):

    def getExtraConnectOptions(self):
        return {}

    def setUp(self):
        super().setUp()

        # Extract options set up with `with_connection_options`.
        test_func = getattr(self, self._testMethodName).__func__
        opts = getattr(test_func, '__connect_options__', {})

        self.con = self.loop.run_until_complete(
            self.cluster.connect(database='postgres', loop=self.loop, **opts))

        self.server_version = self.con.get_server_version()

    def tearDown(self):
        try:
            self.loop.run_until_complete(self.con.close())
            self.con = None
        finally:
            super().tearDown()
