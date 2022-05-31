import pickle
import unittest

import numpy as np
try:
    import pytest
except ImportError:
    raise unittest.SkipTest("This is a benchmark")

from asyncpg.rkt import set_query_dtype
from asyncpg._testbase import ConnectedTestCase


def setup_suite():
    ConnectedTestCase.setUpClass()
    suite = ConnectedTestCase()
    suite._testMethodName = "setUp"
    suite.setUp()
    suite.stmt = None
    return suite


def _entry(suite, coro, length):
    suite.loop.run_until_complete(coro(suite, length))


async def _round_trip(suite, _):
    if suite.stmt is None:
        suite.stmt = await suite.con.prepare("SELECT 1")
    await suite.stmt.fetch()


@pytest.mark.benchmark(
    warmup=True,
    warmup_iterations=2,
)
@pytest.mark.parametrize("length", [1])
def test_min(benchmark, length):
    suite = setup_suite()
    benchmark(_entry, suite, _round_trip, length)


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

# lengths = [100, 200, 500, 1000, 1500, 2000]
lengths = [5000, 10000, 15000, 20000, 30000, 50000]


def compose_query(length):
    value_strs = [sql for _, sql, _ in type_samples]
    return f"SELECT {', '.join(value_strs)}\n" \
           f"FROM generate_series(1, {length}) i"


async def fetch_dummy(suite, length):
    if suite.stmt is None:
        suite.stmt = await suite.con.prepare(
            set_query_dtype(compose_query(length),
                            pickle.dumps(np.dtype(np.void))))
    await suite.stmt.fetch()


@pytest.mark.benchmark(
    warmup=True,
    warmup_iterations=2,
)
@pytest.mark.parametrize("length", lengths)
def test_dummy(benchmark, length):
    suite = setup_suite()
    benchmark(_entry, suite, fetch_dummy, length)


async def fetch_record(suite, length):
    if suite.stmt is None:
        suite.stmt = await suite.con.prepare(compose_query(length))
    await suite.stmt.fetch()


@pytest.mark.benchmark(
    warmup=True,
    warmup_iterations=2,
)
@pytest.mark.parametrize("length", lengths)
def test_record(benchmark, length):
    suite = setup_suite()
    benchmark(_entry, suite, fetch_record, length)


async def fetch_numpy_row(suite, length):
    if suite.stmt is None:
        dtype = np.dtype([(name, subdt) for name, _, subdt in type_samples])
        suite.stmt = await suite.con.prepare(
            set_query_dtype(compose_query(length), dtype))
    await suite.stmt.fetch()


@pytest.mark.benchmark(
    warmup=True,
    warmup_iterations=2,
)
@pytest.mark.parametrize("length", lengths)
def test_numpy_row(benchmark, length):
    suite = setup_suite()
    benchmark(_entry, suite, fetch_numpy_row, length)


async def fetch_numpy_column(suite, length):
    if suite.stmt is None:
        dtype = np.dtype([(name, subdt) for name, _, subdt in type_samples],
                         metadata={"blocks": True})
        suite.stmt = await suite.con.prepare(
            set_query_dtype(compose_query(length), dtype))
    await suite.stmt.fetch()


@pytest.mark.benchmark(
    warmup=True,
    warmup_iterations=2,
)
@pytest.mark.parametrize("length", lengths)
def test_numpy_column(benchmark, length):
    suite = setup_suite()
    benchmark(_entry, suite, fetch_numpy_column, length)
