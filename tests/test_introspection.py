# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from asyncpg import _testbase as tb


MAX_RUNTIME = 0.1


class TestTimeout(tb.ConnectedTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.adminconn = cls.loop.run_until_complete(
            cls.cluster.connect(database='postgres', loop=cls.loop))
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
