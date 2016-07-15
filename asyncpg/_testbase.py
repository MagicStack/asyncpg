import asyncio
import atexit
import contextlib
import functools
import inspect
import logging
import os
import time
import unittest


from asyncpg import cluster as pg_cluster


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

    def setUp(self):
        if os.environ.get('USE_UVLOOP'):
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.loop = loop

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(None)

    @contextlib.contextmanager
    def assertRunLess(self, delta):
        st = time.monotonic()
        try:
            yield
        finally:
            if time.monotonic() - st > delta:
                raise AssertionError(
                    'running block took longer than {}'.format(delta))


_default_cluster = None


def _start_cluster():
    global _default_cluster

    if _default_cluster is None:
        pg_host = os.environ.get('PGHOST')
        if pg_host:
            # Using existing cluster, assuming it is initialized and running
            _default_cluster = pg_cluster.RunningCluster()
        else:
            _default_cluster = pg_cluster.TempCluster()
            _default_cluster.init()
            _default_cluster.start(port=12345)
            atexit.register(_shutdown_cluster, _default_cluster)

    return _default_cluster


def _shutdown_cluster(cluster):
    cluster.stop()
    cluster.destroy()


class ClusterTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.cluster = _start_cluster()


class ConnectedTestCase(ClusterTestCase):

    def setUp(self):
        super().setUp()
        self.con = self.loop.run_until_complete(
            self.cluster.connect(database='postgres', loop=self.loop))

    def tearDown(self):
        try:
            self.loop.run_until_complete(self.con.close())
            self.con = None
        finally:
            super().tearDown()
