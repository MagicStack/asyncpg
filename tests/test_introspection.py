# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import json

from asyncpg import _testbase as tb
from asyncpg import connection as apg_con


MAX_RUNTIME = 0.1


class TestIntrospection(tb.ConnectedTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.adminconn = cls.loop.run_until_complete(cls.connect())
        cls.loop.run_until_complete(
            cls.adminconn.execute('CREATE DATABASE asyncpg_intro_test'))

    @classmethod
    def tearDownClass(cls):
        cls.loop.run_until_complete(
            cls.adminconn.execute('DROP DATABASE asyncpg_intro_test'))

        cls.loop.run_until_complete(cls.adminconn.close())
        cls.adminconn = None

        super().tearDownClass()

    @tb.with_connection_options(database='asyncpg_intro_test')
    async def test_introspection_on_large_db(self):
        await self.con.execute(
            'CREATE TABLE base ({})'.format(
                ','.join('c{:02} varchar'.format(n) for n in range(50))
            )
        )
        for n in range(1000):
            await self.con.execute(
                'CREATE TABLE child_{:04} () inherits (base)'.format(n)
            )

        with self.assertRunUnder(MAX_RUNTIME):
            await self.con.fetchval('SELECT $1::int[]', [1, 2])

    @tb.with_connection_options(statement_cache_size=0)
    async def test_introspection_no_stmt_cache_01(self):
        old_uid = apg_con._uid

        self.assertEqual(self.con._stmt_cache.get_max_size(), 0)
        await self.con.fetchval('SELECT $1::int[]', [1, 2])

        await self.con.execute('''
            CREATE EXTENSION IF NOT EXISTS hstore
        ''')

        try:
            await self.con.set_builtin_type_codec(
                'hstore', codec_name='pg_contrib.hstore')
        finally:
            await self.con.execute('''
                DROP EXTENSION hstore
            ''')

        self.assertEqual(apg_con._uid, old_uid)

    @tb.with_connection_options(max_cacheable_statement_size=1)
    async def test_introspection_no_stmt_cache_02(self):
        # max_cacheable_statement_size will disable caching both for
        # the user query and for the introspection query.
        old_uid = apg_con._uid

        await self.con.fetchval('SELECT $1::int[]', [1, 2])

        await self.con.execute('''
            CREATE EXTENSION IF NOT EXISTS hstore
        ''')

        try:
            await self.con.set_builtin_type_codec(
                'hstore', codec_name='pg_contrib.hstore')
        finally:
            await self.con.execute('''
                DROP EXTENSION hstore
            ''')

        self.assertEqual(apg_con._uid, old_uid)

    @tb.with_connection_options(max_cacheable_statement_size=10000)
    async def test_introspection_no_stmt_cache_03(self):
        # max_cacheable_statement_size will disable caching for
        # the user query but not for the introspection query.
        old_uid = apg_con._uid

        await self.con.fetchval(
            "SELECT $1::int[], '{foo}'".format(foo='a' * 10000), [1, 2])

        self.assertEqual(apg_con._uid, old_uid + 1)

    async def test_introspection_sticks_for_ps(self):
        # Test that the introspected codec pipeline for a prepared
        # statement is not affected by a subsequent codec cache bust.

        ps = await self.con._prepare('SELECT $1::json[]', use_cache=True)

        try:
            # Setting a custom codec blows the codec cache for derived types.
            await self.con.set_type_codec(
                'json', encoder=lambda v: v, decoder=json.loads,
                schema='pg_catalog', format='text'
            )

            # The originally prepared statement should still be OK and
            # use the previously selected codec.
            self.assertEqual(await ps.fetchval(['{"foo": 1}']), ['{"foo": 1}'])

            # The new query uses the custom codec.
            v = await self.con.fetchval('SELECT $1::json[]', ['{"foo": 1}'])
            self.assertEqual(v, [{'foo': 1}])

        finally:
            await self.con.reset_type_codec(
                'json', schema='pg_catalog')
