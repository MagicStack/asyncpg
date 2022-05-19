import random

import numpy as np
from numpy.testing import assert_array_equal

from asyncpg import _testbase as tb
from asyncpg.rkt import set_query_dtype


type_samples = [
    ("bool_false", False, "false", np.bool8),
    ("bool_true", True, "true", np.bool8),
    ("bool_null", True, "null", np.bool8),

    ("int2_32000", 32000, "32000::smallint", np.int16),
    ("int2_1024", -1024, "-1024::smallint", np.int16),
    ("int2_0", 0, "0::smallint", np.int16),
    ("int2_null", -(1 << 15), "null", np.int16),

    ("uint2_32000", 32000, "32000::smallint", np.uint16),
    ("uint2_1024", 64512, "-1024::smallint", np.uint16),
    ("uint2_0", 0, "0::smallint", np.uint16),
    ("uint2_null", 1 << 15, "null", np.uint16),

    ("int4_320000", 3200000, "3200000::int", np.int32),
    ("int4_100024", -100024, "-100024::int", np.int32),
    ("int4_0", 0, "0::int", np.int32),
    ("int4_null", -(1 << 31), "null", np.int32),

    ("uint4_320000", 3200000, "3200000::int", np.uint32),
    ("uint4_100024", 4294867272, "-100024::int", np.uint32),
    ("uint4_0", 0, "0::int", np.uint32),
    ("uint4_null", 1 << 31, "null", np.uint32),

    ("int8_32000000000", 32000000000, "32000000000::bigint", np.int64),
    ("int8_10000000024", -10000000024, "-10000000024::bigint", np.int64),
    ("int8_0", 0, "0::bigint", np.int64),
    ("int8_null", -(1 << 63), "null", np.int64),

    ("uint8_32000000000", 32000000000, "32000000000::bigint", np.uint64),
    ("uint8_10000000024", 18446744063709551592, "-10000000024::bigint",
     np.uint64),
    ("uint8_0", 0, "0::bigint", np.uint64),
    ("uint8_null", 1 << 63, "null", np.uint64),
]


class TestCodecsNumpy(tb.ConnectedTestCase):

    async def test_standard_codecs(self):
        """Test decoding of standard data types to numpy arrays."""
        dtype_body = []
        value_strs = []
        baseline = []
        nulls = []
        shuffled_type_samples = type_samples.copy()
        random.shuffle(shuffled_type_samples)
        for i, (name, value, value_sql, value_dtype) in enumerate(
                shuffled_type_samples):
            baseline.append(value)
            value_strs.append(value_sql)
            dtype_body.append((name, value_dtype))
            if value_sql == "null":
                nulls.append(i)
        baseline = tuple(baseline)
        dtype = np.dtype(dtype_body)
        for length in (1, 512, 513, 1024):
            query = f"SELECT {', '.join(value_strs)}\n" \
                    f"FROM generate_series(1, {length}) i"

            stmt = await self.con.prepare(
                set_query_dtype(query, dtype)
            )
            with self.subTest(length=length):
                fetched_array, fetched_nulls = await stmt.fetch()

                self.assertIsInstance(fetched_nulls, list)
                self.assertEqual(len(fetched_nulls), len(nulls) * length)
                self.assertEqual(fetched_nulls[:len(nulls)], nulls)
                self.assertIsInstance(fetched_array, np.ndarray)
                assert_array_equal(
                    fetched_array, np.array([baseline] * length, dtype=dtype))
