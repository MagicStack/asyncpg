import asyncio

from asyncpg import _testbase as tb


class TestQueryLogging(tb.ConnectedTestCase):

    async def test_logging_context(self):
        queries = asyncio.Queue()

        def query_saver(conn, record):
            queries.put_nowait(record)

        with self.con.logger(query_saver):
            self.assertEqual(len(self.con._query_loggers), 1)
            await self.con.execute("SELECT 1")

        record = await queries.get()
        self.assertEqual(record.query, "SELECT 1")
        self.assertEqual(len(self.con._query_loggers), 0)
