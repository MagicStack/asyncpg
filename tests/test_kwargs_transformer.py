# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import unittest

from asyncpg.query_pp import transform_kwargs  # NOQA


class TestKwargsTransformer(unittest.TestCase):

    queries = [
        ('select 1',
         'select 1',
         None),

        ('select $a',
         'select $1',
         ('a',)),

        ('select $1',
         'select $1',
         None),

        ('select $1 + "$1" + \'$1\' + $2',
         'select $1 + "$1" + \'$1\' + $2',
         None),

        ('select $abc1',
         'select $1',
         ('abc1',)),

        ('select $abc1  ',
         'select $1  ',
         ('abc1',)),

        ('''
            SELECT """" + $foo + "$foo"""
         ''',
         '''
            SELECT """" + $1 + "$foo"""
         ''',
         ('foo',)),

        ('''
            SELECT """" + $foo + "$foo""" + -- $bar "
                - $baz
         ''',
         '''
            SELECT """" + $1 + "$foo""" + -- $bar "
                - $2
         ''',
         ('foo', 'baz')),

        ('''
            SELECT """" + $1 + "$foo""" + -- $bar "
                - $2
         ''',
         '''
            SELECT """" + $1 + "$foo""" + -- $bar "
                - $2
         ''',
         None),

        (r'''
            SELECT E'\'' + $foo + E'$foo\'' + -- $bar "
                - $baz
         ''',
         r'''
            SELECT E'\'' + $1 + E'$foo\'' + -- $bar "
                - $2
         ''',
         ('foo', 'baz')),

        (r'''
            SELECT e'\'' + $foo + e'$foo\'' + -- $bar "
                - $baz
         ''',
         r'''
            SELECT e'\'' + $1 + e'$foo\'' + -- $bar "
                - $2
         ''',
         ('foo', 'baz')),

        (r'''
            SELECT '\' || $foo + '$foo\' + $fiz -- $bar "
                - $baz
         ''',
         r'''
            SELECT '\' || $1 + '$foo\' + $2 -- $bar "
                - $3
         ''',
         ('foo', 'fiz', 'baz')),

        (r'''
            SELECT CASE'\' WHEN $a THEN $b ELSE $a END;
         ''',
         r'''
            SELECT CASE'\' WHEN $1 THEN $2 ELSE $1 END;
         ''',
         ('a', 'b')),

        (r'''
            SELECT case'\' WHEN $a THEN $b ELSE $a END;
         ''',
         r'''
            SELECT case'\' WHEN $1 THEN $2 ELSE $1 END;
         ''',
         ('a', 'b')),

        ("""
            SELECT 'Baz''' + $foo + /* x *
                - $y + $z **// $a
         """,
         """
            SELECT 'Baz''' + $1 + /* x *
                - $y + $z **// $2
         """,
         ('foo', 'a')),

        ("""
            SELECT 'Baz' 'fiz' + $foo + /* x *
                - $y + $z **// $a
         """,
         """
            SELECT 'Baz' 'fiz' + $1 + /* x *
                - $y + $z **// $2
         """,
         ('foo', 'a')),

        ("""
            SELECT $$'Baz''' + $foo + /* x * $$
                - $y + $z **// $a
         """,
         """
            SELECT $$'Baz''' + $foo + /* x * $$
                - $1 + $2 **// $3
         """,
         ('y', 'z', 'a')),

        ("""
            SELECT $abc_a$'Baz''' + $foo + /* x * $abc_a$
                - $y + $z **// $a
         """,
         """
            SELECT $abc_a$'Baz''' + $foo + /* x * $abc_a$
                - $1 + $2 **// $3
         """,
         ('y', 'z', 'a')),
    ]

    # We should ignore any errors in queries.
    invalid_queries = [
        ('',
         '',
         None),

        ('расколбас $f',
         'расколбас $1',
         ('f',)),

        (' ',
         ' ',
         None),

        ('  ',
         '  ',
         None),

        ('$',
         '$',
         None),

        ('$$',
         '$$',
         None),

        ('$$$',
         '$$$',
         None),

        ('$$$$',
         '$$$$',
         None),

        ('$$$$$',
         '$$$$$',
         None),

        ('"',
         '"',
         None),

        ('""',
         '""',
         None),

        ('"""',
         '"""',
         None),

        ('""""',
         '""""',
         None),

        ('"""""',
         '"""""',
         None),

        ('e',
         'e',
         None),

        ('e"',
         'e"',
         None),

        ("e'",
         "e'",
         None),

        ('select $',
         'select $',
         None),

        ('select $ ',
         'select $ ',
         None),

        ('select $ as',
         'select $ as',
         None),

        ('select $ $as',
         'select $ $1',
         ('as',)),

        ('select "as',
         'select "as',
         None),

        ("select 'as",
         "select 'as",
         None),

        ("select 'as\\",
         "select 'as\\",
         None),

        ("select $foo + 'as\'a ",
         "select $1 + 'as\'a ",
         ('foo',)),

        ("select $foo + E'as\'a ",
         "select $1 + E'as\'a ",
         ('foo',)),

        ("select $$as",
         "select $$as",
         None),

        ("select $foo$as",
         "select $foo$as",
         None),
    ]

    # Can't combine named and positional-only arguments.
    invalid_params_combos = [
        'select $foo + $1',
        'select $1 + $foo'
    ]

    # Invalid arguments names
    invalid_names = [
        'select $1foo',
    ]

    def test_params_lex_valid_queries(self):
        for query, expected_query, expected_params in self.queries:
            with self.subTest(query=query):
                new_query, new_params = transform_kwargs(query)
                self.assertEqual(new_query, expected_query)
                self.assertEqual(new_params, expected_params)

    def test_params_lex_invalid_queries(self):
        for query, expected_query, expected_params in self.invalid_queries:
            with self.subTest(query=query):
                new_query, new_params = transform_kwargs(query)
                self.assertEqual(new_query, expected_query)
                self.assertEqual(new_params, expected_params)

    def test_params_lex_param_combination(self):
        for query in self.invalid_params_combos:
            with self.subTest(query=query):
                with self.assertRaisesRegex(ValueError, 'queries with both'):
                    transform_kwargs(query)

    def test_params_lex_invalid_names(self):
        for query in self.invalid_names:
            with self.subTest(query=query):
                with self.assertRaisesRegex(ValueError,
                                            'invalid argument name'):
                    transform_kwargs(query)

    def test_params_lex_worstcase_args(self):

        def count_base61():
            ar = ('abcdefghijklmnopqrstuvwxyz'
                  'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                  '_')
            arl = len(ar)

            far = ar + '0123456789'
            farl = len(far)

            i = 0
            while True:
                num = i
                buf = ''

                pos = num % arl
                num = num // arl
                buf += ar[pos]

                while num:
                    pos = num % farl
                    num = num // farl
                    buf += far[pos]

                yield buf
                i += 1

        # Build a query that has 33000 unique and shortest possible
        # named arguments.
        query = 'SELECT '
        expected_params = []
        expected_query = query
        for i, name in enumerate(count_base61()):
            if i == 33000:  # Max number of arguments Postgres can accept.
                break
            query += f'${name}+'
            expected_query += f'${i + 1}+'
            expected_params.append(name)

        query = query[:-1]
        expected_query = expected_query[:-1]

        new_query, params = transform_kwargs(query)
        self.assertEqual(new_query, expected_query)
        self.assertEqual(params, tuple(expected_params))

        # We use '1.4' as a coefficient when we pre-allocate a buffer
        # for the new query in `query_pp.pyx`.
        self.assertLess(len(new_query) / len(query), 1.4)
