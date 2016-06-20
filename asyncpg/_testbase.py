import asyncio
import asyncpg
import functools
import inspect
import unittest


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
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.loop = loop

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(None)


class ConnectedTestCase(TestCase):

    def setUp(self):
        super().setUp()
        self.con = self.loop.run_until_complete(
            asyncpg.connect(loop=self.loop))

    def tearDown(self):
        try:
            self.con.close()
            self.con = None
        finally:
            super().tearDown()
