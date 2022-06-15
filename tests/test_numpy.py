from datetime import date, datetime, time, timedelta
import decimal
import ipaddress
import random
from uuid import UUID

import numpy as np
from numpy.testing import assert_array_equal

from asyncpg import _testbase as tb, DTypeError
from asyncpg.rkt import set_query_dtype


type_samples = [
    ("bool_false", False, "false", np.bool8),
    ("bool_true", True, "true", np.bool8),
    ("bool_obj", True, "true", object),
    ("bool_null", True, "null", np.bool8),

    ("int2_32000", 32000, "32000::smallint", np.int16),
    ("int2_1024", -1024, "-1024::smallint", np.int16),
    ("int2_0", 0, "0::smallint", np.int16),
    ("int2_obj", 32000, "32000::smallint", object),
    ("int2_null", -(1 << 15), "null", np.int16),

    ("uint2_32000", 32000, "32000::smallint", np.uint16),
    ("uint2_1024", 64512, "-1024::smallint", np.uint16),
    ("uint2_0", 0, "0::smallint", np.uint16),
    ("uint2_null", 1 << 15, "null", np.uint16),

    ("int4_320000", 3200000, "3200000::int", np.int32),
    ("int4_100024", -100024, "-100024::int", np.int32),
    ("int4_0", 0, "0::int", np.int32),
    ("int4_obj", 3200000, "3200000::int", object),
    ("int4_null", -(1 << 31), "null", np.int32),

    ("uint4_320000", 3200000, "3200000::int", np.uint32),
    ("uint4_100024", 4294867272, "-100024::int", np.uint32),
    ("uint4_0", 0, "0::int", np.uint32),
    ("uint4_null", 1 << 31, "null", np.uint32),

    ("int8_32000000000", 32000000000, "32000000000::bigint", np.int64),
    ("int8_10000000024", -10000000024, "-10000000024::bigint", np.int64),
    ("int8_0", 0, "0::bigint", np.int64),
    ("int8_obj", 32000000000, "32000000000::bigint", object),
    ("int8_null", -(1 << 63), "null", np.int64),

    ("uint8_32000000000", 32000000000, "32000000000::bigint", np.uint64),
    ("uint8_10000000024", 18446744063709551592, "-10000000024::bigint",
     np.uint64),
    ("uint8_0", 0, "0::bigint", np.uint64),
    ("uint8_null", 1 << 63, "null", np.uint64),

    ("float", 2.125, "2.125::float4", np.float32),
    ("floatnan", np.nan, "'NaN'::float4", np.float32),
    ("floatinf", np.inf, "'inf'::float4", np.float32),
    ("floatneginf", -np.inf, "-'inf'::float4", np.float32),
    ("floatnull", np.nan, "null", np.float32),
    ("floatobj", 2.125, "2.125::float4", object),

    ("double", 2.125, "2.125::float8", np.float64),
    ("doublenan", np.nan, "'NaN'::float8", np.float64),
    ("doubleinf", np.inf, "'inf'::float8", np.float64),
    ("doubleneginf", -np.inf, "-'inf'::float8", np.float64),
    ("doublenull", np.nan, "null", np.float64),
    ("doubleobj", 2.125, "2.125::float8", object),

    ("numeric", decimal.Decimal(10000), "10000::numeric", object),

    ("bytea0", b"", "''::bytea", "S1"),
    ("bytea5", b"12345", "'12345'::bytea", "S5"),
    ("bytea10", b"12345", "'12345'::bytea", "S10"),
    ("byteanull", b"\xff", "null", "S1"),
    ("byteaobj", b"12345", "'12345'::bytea", object),

    ("text0", "", "''::text", "U1"),
    ("text5", "12345", "'12345'::text", "U5"),
    ("text10", "12345", "'12345'::text", "U10"),
    ("textrocket", "🚀áño", "'🚀áño'::text", "U9"),
    ("textnull", "", "null", "U1"),
    ("textobj", "12345", "'12345'::text", object),

    ("char", b"1", "'1'::char", "S1"),

    ("dtslt", np.datetime64("1989-01-12 12:00:01", "s"),
     "'1989-01-12 12:00:01.123'::timestamp", "datetime64[s]"),
    ("dtsgt", np.datetime64("1989-01-12 12:00:01", "s"),
     "'1989-01-12 12:00:01.723'::timestamp", "datetime64[s]"),
    ("dtms", np.datetime64("1989-01-12 12:00:01.123", "ms"),
     "'1989-01-12 12:00:01.123'::timestamp", "datetime64[ms]"),
    ("dtsz", np.datetime64("1989-01-12 11:00:01", "s"),
     "'1989-01-12 12:00:01.123+01:00'::timestamptz", "datetime64[s]"),
    ("dtmsz", np.datetime64("1989-01-12 11:00:01.123", "ms"),
     "'1989-01-12 12:00:01.123+01:00'::timestamptz", "datetime64[ms]"),
    ("dt1800lt", np.datetime64("1800-01-12 12:00:01", "s"),
     "'1800-01-12 12:00:01.123'::timestamp", "datetime64[s]"),
    ("dt1800gt", np.datetime64("1800-01-12 12:00:01", "s"),
     "'1800-01-12 12:00:01.723'::timestamp", "datetime64[s]"),
    ("dt1800ex", np.datetime64("1800-01-12 12:00:01", "s"),
     "'1800-01-12 12:00:01'::timestamp", "datetime64[s]"),
    ("dtmax", np.datetime64("294247-01-10T04:00:54.775807", "us"),
     "'infinity'::timestamp", "datetime64[us]"),
    ("dtmin", np.datetime64("-290308-12-21T19:59:05.224193", "us"),
     "'-infinity'::timestamp", "datetime64[us]"),
    ("dtnull", np.datetime64("NaT", "us"), "null", "datetime64[us]"),
    ("dtobj", datetime(1989, 1, 12, 12, 0, 1, microsecond=123000),
     "'1989-01-12 12:00:01.123'::timestamp", object),

    ("date", np.datetime64("1989-01-12", "D"),
     "'1989-01-12'::date", "datetime64[D]"),
    ("datenull", np.datetime64("NaT", "D"), "null", "datetime64[D]"),
    ("dateobj", date(1989, 1, 12), "'1989-01-12'::date", object),

    ("time100", np.timedelta64(100, "s"), "'00:01:40.123'::time",
     "timedelta64[s]"),
    ("time_100", np.timedelta64(-100, "s"), "-'00:01:40.123'::time",
     "timedelta64[s]"),
    ("time0", np.timedelta64(0, "us"), "'00:00:00'::time",
     "timedelta64[us]"),
    ("timenull", np.timedelta64("NaT", "us"), "null", "timedelta64[us]"),
    ("timeobj", time(0, 1, 40, 123000), "'00:01:40.123'::time",
     object),

    ("timetz100", np.timedelta64(-3500, "s"), "'00:01:40+01:00'::timetz",
     "timedelta64[s]"),

    ("interval", np.timedelta64(10, "D"), "'10 days'::interval",
     "timedelta64[D]"),
    ("intervalobj", timedelta(days=10), "'10 days'::interval", object),

    ("uuid", b"\x07" * 16, "'07070707-0707-0707-0707-070707070707'::uuid",
     "S16"),
    ("uuidobj", UUID(bytes=b"\x07" * 16),
     "'07070707-0707-0707-0707-070707070707'::uuid", object),

    ("varbit8", b"\x10", "'00010000'::varbit", "S1"),
    ("varbit9", b"\x10\x80", "'000100001'::varbit", "S2"),
    ("varbitobj", b"\x10", "'00010000'::varbit", object),

    ("tid", (10, 20), "'(10, 20)'::tid",
     np.dtype([("major", np.int32), ("minor", np.int16)])),

    ("oid", 987123, "987123::oid", np.uint32),

    ("point", (1.0, 2.0), "'(1.0, 2.0)'::point",
     np.dtype([("x", float), ("y", float)])),
    ("box", (3.0, 4.0, 1.0, 2.0), "'(1.0, 2.0, 3.0, 4.0)'::box",
     np.dtype([("high_x", float), ("high_y", float),
               ("low_x", float), ("low_y", float)])),
    ("lseg", (1.0, 2.0, 3.0, 4.0), "'(1.0, 2.0, 3.0, 4.0)'::lseg",
     np.dtype([("high_x", float), ("high_y", float),
               ("low_x", float), ("low_y", float)])),
    ("line", (1.0, 2.0, 3.0), "'{1.0, 2.0, 3.0}'::line",
     np.dtype([("a", float), ("b", float), ("c", float)])),
    ("circle", (1.0, 2.0, 3.0), "'1.0, 2.0, 3.0'::circle",
     np.dtype([("a", float), ("b", float), ("c", float)])),

    ("inet", ipaddress.IPv4Address('127.0.0.1'), "'127.0.0.1'::inet",
     object),
]

error_type_samples = [
    ("sanity", "1::int", np.int32, None),
    ("int", "7::int", bool, DTypeError),
    ("int64", "7::bigint", np.int32, DTypeError),
    ("int64", "7::bigint", "datetime64[s]", DTypeError),
    ("bytea", "'1234'::bytea", "S3", DTypeError),
    ("text", "'1234'::text", "U3", DTypeError),
    ("float", "1.0::float8", np.float32, DTypeError),
    ("varbit9", "'000100001'::varbit", "S1", DTypeError),
    ("dts", "'1989-01-12 12:00:01.123'::timestamp", int, DTypeError),
]


class TestCodecsNumpy(tb.ConnectedTestCase):

    def setup_standard_codecs(self, blocks: bool):
        dtype_body = []
        value_strs = []
        baseline = []
        nulls = []
        nans = []
        shuffled_type_samples = type_samples.copy()
        random.shuffle(shuffled_type_samples)
        for i, (name, value, value_sql, value_dtype) in enumerate(
                shuffled_type_samples):
            baseline.append(value)
            value_strs.append(value_sql)
            dtype_body.append((name, value_dtype))
            if value_sql == "null":
                nulls.append(i)
            if value != value:
                nans.append(i)
        baseline = tuple(baseline)
        if blocks:
            dtype = np.dtype(dtype_body, metadata={"blocks": True})
        else:
            dtype = np.dtype(dtype_body)
        return dtype, baseline, value_strs, nulls, nans

    async def test_standard_codecs_row(self):
        """Test decoding of standard data types to numpy arrays, row-major."""
        dtype, baseline, value_strs, nulls, nans = \
            self.setup_standard_codecs(False)
        for length in (0, 1, 512, 513, 1024):
            query = f"SELECT {', '.join(value_strs)}\n" \
                    f"FROM generate_series(1, {length}) i"

            stmt = await self.con.prepare(
                set_query_dtype(query, dtype)
            )
            with self.subTest(length=length):
                fetched_array, fetched_nulls = await stmt.fetch()

                self.assertIsInstance(fetched_nulls, list)
                self.assertEqual(len(fetched_nulls), len(nulls) * length)
                if length > 0:
                    self.assertEqual(fetched_nulls[:len(nulls)], nulls)
                self.assertIsInstance(fetched_array, np.ndarray)
                nan_mask = np.zeros(len(dtype), dtype=bool)
                nan_mask[nulls] = True
                nan_mask[nans] = True
                baseline_array = np.array([baseline] * length, dtype=dtype)
                # https://github.com/numpy/numpy/issues/21539
                for is_nan, (key, (child_dtype, _)) in zip(
                        nan_mask, dtype.fields.items()):
                    if not is_nan:
                        continue
                    if child_dtype.kind in ("m", "M", "f"):
                        assert_array_equal(
                            fetched_array[key], baseline_array[key])
                        fetched_array[key] = baseline_array[key] = 0
                assert_array_equal(fetched_array, baseline_array)

    async def test_standard_codecs_column(self):
        """Test decoding of standard data types to numpy arrays,
        column-major."""
        dtype, baseline, value_strs, nulls, _ = \
            self.setup_standard_codecs(True)
        for length in (0, 1, 512, 513, 1024):
            query = f"SELECT {', '.join(value_strs)}\n" \
                    f"FROM generate_series(1, {length}) i"

            stmt = await self.con.prepare(
                set_query_dtype(query, dtype)
            )
            with self.subTest(length=length):
                fetched_array, fetched_nulls = await stmt.fetch()

                self.assertIsInstance(fetched_nulls, list)
                self.assertEqual(len(fetched_nulls), len(nulls) * length)
                if length > 0:
                    self.assertEqual(fetched_nulls[:len(nulls)], nulls)
                    if length > 1:
                        fetched_nulls_chunk = fetched_nulls[
                            len(nulls):len(nulls) * 2
                        ]
                        nulls_chunk = (
                            np.array(nulls) + len(dtype)
                        ).tolist()
                        self.assertEqual(
                            fetched_nulls_chunk,
                            nulls_chunk)
                        if length > 512:
                            fetched_nulls_chunk = fetched_nulls[
                                len(nulls) * 512:len(nulls) * 513
                            ]
                            nulls_chunk = (
                                np.array(nulls) + 512 * len(dtype)
                            ).tolist()
                            self.assertEqual(
                                fetched_nulls_chunk,
                                nulls_chunk)
                self.assertIsInstance(fetched_array, np.void)
                for i, name in enumerate(dtype.names):
                    baseline_array = np.array(
                        [baseline[i]] * length, dtype=dtype.fields[name][0])
                    assert_array_equal(fetched_array[i], baseline_array)

    async def test_exceptions(self):
        """Test decoding of standard data types to numpy arrays."""
        for name, value_sql, value_dtype, exc in error_type_samples:
            dtype = np.dtype([(name, value_dtype)])
            query = set_query_dtype(f"SELECT {value_sql}", dtype)
            with self.subTest(name=name):
                if exc is not None:
                    with self.assertRaises(exc):
                        await self.con.fetch(query)
                else:
                    await self.con.fetch(query)

    async def test_empty_query(self):
        """Test return nothing with an empty query and a dtype."""
        dtype = np.dtype([("sha", "S40")])
        result = await self.con.fetch(set_query_dtype("", dtype))
        assert_array_equal(result[0], np.array([], dtype=dtype))
        assert result[1] == []
