import asyncpg
from asyncpg import _testbase as tb


class TestExceptions(tb.ConnectedTestCase):

    def test_exceptions_exported(self):
        self.assertTrue(hasattr(asyncpg, 'ConnectionError'))
        self.assertIn('ConnectionError', asyncpg.__all__)

    async def test_exceptions_unpacking(self):
        with self.assertRaises(asyncpg.Error):
            try:
                await self.con.execute('SELECT * FROM _nonexistent_')
            except asyncpg.Error as e:
                self.assertEqual(e.sqlstate, '42P01')
                self.assertEqual(e.position, '15')
                self.assertEqual(e.query, 'SELECT * FROM _nonexistent_')
                self.assertIsNotNone(e.severity)
                raise
