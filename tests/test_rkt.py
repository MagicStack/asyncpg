import numpy as np

from asyncpg import _testbase as tb
from asyncpg.rkt import set_query_dtype


class TestRocket(tb.TestCase):

    def test_rkt_smoke(self):
        query = "SELECT 1, 1"
        dtype = np.dtype([("a", int), ("b", int)])
        augmented_query = set_query_dtype(query, dtype)
        self.assertEqual(augmented_query[:3], "--🚀")
        end = augmented_query.find("🚀\n", 1)
        header = augmented_query[3:end]
        self.assertEqual(augmented_query[end + 2:], query)
        for char in header:
            self.assertIn(char, "0123456789abcdef")

    def test_rkt_wrong_type(self):
        with self.assertRaises(TypeError):
            set_query_dtype("SELECT 1", 1)
        with self.assertRaises(ValueError):
            set_query_dtype("SELECT 1", None)

    def test_rkt_wrong_dtype(self):
        with self.assertRaises(ValueError):
            set_query_dtype("SELECT 1", np.int64)
