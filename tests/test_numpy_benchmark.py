import os

from asyncpg.rkt import set_query_dtype
import numpy as np
from asyncpg._testbase import ConnectedTestCase


def setup_suite():
    ConnectedTestCase.setUpClass()
    suite = ConnectedTestCase()
    suite._testMethodName = "setUp"
    suite.setUp()
    suite.stmt = None
    return suite


def _entry(suite, coro):
    suite.loop.run_until_complete(coro(suite))


async def round_trip(suite):
    if suite.stmt is None:
        suite.stmt = await suite.con.prepare("SELECT 1")
    await suite.stmt.fetch()


def test_round_trip(benchmark):
    suite = setup_suite()
    benchmark(_entry, suite, round_trip)


type_samples = [
    ("bool1", "false", np.bool8),
    ("bool2", "true", np.bool8),

    ("int1", "32000000000::bigint", np.int64),
    ("int2", "32000000001::bigint", np.int64),
    ("int3", "32000000002::bigint", np.int64),
    ("int4", "32000000003::bigint", np.int64),
    ("int5", "32000000004::bigint", np.int64),

    ("float", "2.125::float4", np.float32),

    ("bytes1", "'\\x%s'::bytea" % ("01" * 16), "S16"),
    ("bytes2", "'\\x%s'::bytea" % ("02" * 16), "S16"),

    ("text1", "'12345'::text", "U5"),
    ("text2", "'12345'::text", "U10"),

    ("dt1", "'1989-01-12 12:00:01.123'::timestamp", "datetime64[s]"),
    ("dt2", "'2022-01-12 12:00:01.123'::timestamp", "datetime64[us]"),
    ("dt3", "'2021-01-12 12:00:01.123'::timestamp", "datetime64[us]"),
    ("dt4", "'2020-01-12 12:00:01.123'::timestamp", "datetime64[us]"),

    ("time1", "'00:01:40'::time", "timedelta64[s]"),
    ("time2", "'01:01:40'::time", "timedelta64[us]"),
]


def compose_query():
    length = int(os.getenv("SELECT_ROWS", "1000"))
    value_strs = [sql for _, sql, _ in type_samples]
    return f"SELECT {', '.join(value_strs)}\n" \
           f"FROM generate_series(1, {length}) i"


async def fetch_records(suite):
    if suite.stmt is None:
        suite.stmt = await suite.con.prepare(compose_query())
    await suite.stmt.fetch()


def test_fetch_records(benchmark):
    suite = setup_suite()
    benchmark(_entry, suite, fetch_records)


async def fetch_numpy(suite):
    if suite.stmt is None:
        dtype = np.dtype([(name, subdt) for name, _, subdt in type_samples])
        suite.stmt = await suite.con.prepare(
            set_query_dtype(compose_query(), dtype))
    await suite.stmt.fetch()


def test_fetch_numpy(benchmark):
    suite = setup_suite()
    benchmark(_entry, suite, fetch_numpy)
