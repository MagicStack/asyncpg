import contextlib
import gc
import pickle
import sys
import unittest

from asyncpg.protocol.protocol import _create_record as Record


class TestRecord(unittest.TestCase):

    @contextlib.contextmanager
    def checkref(self, *objs):
        cnt = [sys.getrefcount(objs[i]) for i in range(len(objs))]
        yield
        for _ in range(3):
            gc.collect()
        for i in range(len(objs)):
            before = cnt[i]
            after = sys.getrefcount(objs[i])
            if before != after:
                self.fail('refcounts differ for {!r}: {:+}'.format(
                    objs[i], after - before))

    def test_record_invalid_args(self):
        with self.assertRaises(SystemError):
            Record({}, ())

    def test_record_gc(self):
        elem = object()
        mapping = {}
        with self.checkref(mapping, elem):
            r = Record(mapping, (elem,))
            del r

        key = 'spam'
        val = int('101010')
        mapping = {key: val}
        with self.checkref(key, val):
            r = Record(mapping, (0,))
            with self.assertRaises(KeyError):
                r[key]
            del r

        key = 'spam'
        val = 'ham'
        mapping = {key: val}
        with self.checkref(key, val):
            r = Record(mapping, (0,))
            with self.assertRaises(KeyError):
                r[key]
            del r

    def test_record_freelist_ok(self):
        for _ in range(10000):
            Record({'a': 0}, (42,))
            Record({'a': 0, 'b': 1}, (42, 42,))

    def test_record_len_getindex(self):
        r = Record({'a': 0}, (42,))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0], 42)
        self.assertEqual(r['a'], 42)

        r = Record({'a': 0, 'b': 1}, (42, 43))
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 42)
        self.assertEqual(r[1], 43)
        self.assertEqual(r['a'], 42)
        self.assertEqual(r['b'], 43)

        with self.assertRaisesRegex(IndexError,
                                    'record index out of range'):
            r[1000]

        with self.assertRaisesRegex(KeyError, 'spam'):
            r['spam']

        with self.assertRaisesRegex(KeyError, 'spam'):
            Record(None, (1,))['spam']

        with self.assertRaisesRegex(KeyError, 'spam'):
            Record({'spam': 123}, (1,))['spam']

    def test_record_immutable(self):
        r = Record({'a': 0}, (42,))
        with self.assertRaisesRegex(TypeError, 'does not support item'):
            r[0] = 1

    def test_record_repr(self):
        r = Record({'a': 0}, (42,))
        self.assertTrue(repr(r).startswith('<Record '))

    def test_record_iter(self):
        r = Record({'a': 0, 'b': 1}, (42, 43))
        self.assertEqual(tuple(r), (42, 43))

    def test_record_values(self):
        r = Record({'a': 0, 'b': 1}, (42, 43))
        vv = r.values()
        self.assertEqual(tuple(vv), (42, 43))
        self.assertTrue(repr(vv).startswith('<RecordIterator '))

    def test_record_keys(self):
        r = Record({'a': 0, 'b': 1}, (42, 43))
        vv = r.keys()
        self.assertEqual(tuple(vv), ('a', 'b'))

    def test_record_hash(self):
        r1 = Record({'a': 0, 'b': 1}, (42, 43))
        r2 = Record({'a': 0, 'b': 1}, (42, 43))
        r3 = Record({'a': 0, 'b': 1}, (42, 45))
        r4 = (42, 43)

        self.assertEqual(hash(r1), hash(r2))
        self.assertNotEqual(hash(r1), hash(r3))
        self.assertNotEqual(hash(r1), hash(r4))

        d = {}
        d[r1] = 123
        self.assertEqual(d[r1], 123)
        self.assertIn(r2, d)
        self.assertEqual(d[r2], 123)
        self.assertNotIn(r3, d)
        self.assertNotIn(r4, d)

    def test_record_cmp(self):
        r1_map = {'a': 0, 'b': 1}
        r1 = Record(r1_map, (42, 43))
        r2 = Record(r1_map, (42, 43))
        r3 = Record({'a': 0, 'b': 1}, (42, 43))

        r4 = Record({'a': 0, 'b': 1}, (42, 45))
        r5 = Record({'a': 0, 'b': 1, 'c': 2}, (42, 46, 57))
        r6 = Record({'a': 0, 'c': 1}, (42, 43))

        r7 = (42, 43)

        self.assertEqual(r1, r2)
        self.assertEqual(r1, r3)

        self.assertNotEqual(r1, r4)
        self.assertNotEqual(r1, r5)
        self.assertNotEqual(r1, r6)
        self.assertNotEqual(r1, r7)
        self.assertNotEqual(r4, r5)
        self.assertNotEqual(r4, r6)
        self.assertNotEqual(r6, r5)

        self.assertLess(r1, r4)
        self.assertGreater(r4, r1)

        self.assertLess(r1, r5)
        self.assertGreater(r5, r6)
        self.assertGreater(r5, r4)

        with self.assertRaisesRegex(TypeError, 'unorderable'):
            r7 < r1

        with self.assertRaisesRegex(TypeError, 'unorderable'):
            r1 < r7

    def test_record_not_pickleable(self):
        r = Record({'a': 0}, (42,))
        with self.assertRaisesRegex(TypeError,
                                    "can't pickle Record objects"):
            pickle.dumps(r)
