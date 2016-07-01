import asyncpg
from asyncpg import _testbase as tb


class TestExceptions(tb.ConnectedTestCase):

    def test_exceptions_exported(self):
        self.assertTrue(hasattr(asyncpg, 'ConnectionError'))
        self.assertIn('ConnectionError', asyncpg.__all__)
