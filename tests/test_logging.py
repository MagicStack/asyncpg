import asyncio

from asyncpg import _testbase as tb
from asyncpg import exceptions


class LogCollector:
    def __init__(self):
        self.records = []

    def __call__(self, record):
        self.records.append(record)


class TestQueryLogging(tb.ConnectedTestCase):

    async def test_logging_context(self):
        queries = asyncio.Queue()

        def query_saver(record):
            queries.put_nowait(record)

        with self.con.logger(query_saver):
            self.assertEqual(len(self.con._query_loggers), 1)
            await self.con.execute("SELECT 1")
            with self.con.logger(LogCollector()) as log:
                self.assertEqual(len(self.con._query_loggers), 2)
                await self.con.execute("SELECT 2")

        r1 = await queries.get()
        r2 = await queries.get()
        self.assertEqual(r1.query, "SELECT 1")
        self.assertEqual(r2.query, "SELECT 2")
        self.assertEqual(len(log.records), 1)
        self.assertEqual(log.records[0].query, "SELECT 2")
        self.assertEqual(len(self.con._query_loggers), 0)

    async def test_error_logging(self):
        with self.con.logger(LogCollector()) as log:
            with self.assertRaises(exceptions.UndefinedColumnError):
                await self.con.execute("SELECT x")

        await asyncio.sleep(0)  # wait for logging
        self.assertEqual(len(log.records), 1)
        self.assertEqual(
            type(log.records[0].exception),
            exceptions.UndefinedColumnError
        )
