import unittest
from unittest import mock

from asyncpg import connection as connection_mod


class DummyConnection:
    _stmt_cache_enabled = False

    def _check_open(self):
        pass

    async def _get_statement(self, query, timeout, **kwargs):
        self.query = query
        self.named = kwargs["named"]
        return object()


class TestExplicitPrepareNaming(unittest.IsolatedAsyncioTestCase):
    async def test_prepare_without_name_stays_named_when_cache_disabled(self):
        connection = DummyConnection()
        prepared = object()

        with mock.patch.object(
            connection_mod.prepared_stmt,
            "PreparedStatement",
            return_value=prepared,
        ):
            result = await connection_mod.Connection._prepare(
                connection,
                "select 1",
            )

        self.assertIs(result, prepared)
        self.assertEqual(connection.query, "select 1")
        self.assertIs(connection.named, True)
