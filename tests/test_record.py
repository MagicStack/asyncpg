import contextlib
import gc
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

        with self.assertRaisesRegex(KeyError, 'spam'):
            r['spam']

        with self.assertRaisesRegex(KeyError, 'spam'):
            Record(None, (1,))['spam']

        with self.assertRaisesRegex(KeyError, 'spam'):
            Record({'spam': 123}, (1,))['spam']

    def test_record_repr(self):
        r = Record({'a': 0}, (42,))
        self.assertTrue(repr(r).startswith('<Record '))
