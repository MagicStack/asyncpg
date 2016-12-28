# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio

from asyncpg import _testbase as tb
from asyncpg.exceptions import PostgresNotice, PostgresWarning


class TestListeners(tb.ClusterTestCase):

    async def test_listen_01(self):
        async with self.create_pool(database='postgres') as pool:
            async with pool.acquire() as con:

                q1 = asyncio.Queue(loop=self.loop)
                q2 = asyncio.Queue(loop=self.loop)

                def listener1(*args):
                    q1.put_nowait(args)

                def listener2(*args):
                    q2.put_nowait(args)

                await con.add_listener('test', listener1)
                await con.add_listener('test', listener2)

                await con.execute("NOTIFY test, 'aaaa'")

                self.assertEqual(
                    await q1.get(),
                    (con, con.get_server_pid(), 'test', 'aaaa'))
                self.assertEqual(
                    await q2.get(),
                    (con, con.get_server_pid(), 'test', 'aaaa'))

                await con.remove_listener('test', listener2)

                await con.execute("NOTIFY test, 'aaaa'")

                self.assertEqual(
                    await q1.get(),
                    (con, con.get_server_pid(), 'test', 'aaaa'))
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(q2.get(),
                                           timeout=0.05, loop=self.loop)

                await con.reset()
                await con.remove_listener('test', listener1)
                await con.execute("NOTIFY test, 'aaaa'")

                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(q1.get(),
                                           timeout=0.05, loop=self.loop)
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(q2.get(),
                                           timeout=0.05, loop=self.loop)

    async def test_listen_02(self):
        async with self.create_pool(database='postgres') as pool:
            async with pool.acquire() as con1, pool.acquire() as con2:

                q1 = asyncio.Queue(loop=self.loop)

                def listener1(*args):
                    q1.put_nowait(args)

                await con1.add_listener('ipc', listener1)
                await con2.execute("NOTIFY ipc, 'hello'")

                self.assertEqual(
                    await q1.get(),
                    (con1, con2.get_server_pid(), 'ipc', 'hello'))


class TestNotices(tb.ConnectedTestCase):
    async def test_notify_01(self):
        q1 = asyncio.Queue(loop=self.loop)

        def notice_callb(con, message):
            # data in the message depend on PG's version, hide some values
            if message.server_source_line is not None :
                message.server_source_line = '***'

            q1.put_nowait((con, type(message), message.as_dict()))

        con = self.con
        con.add_notice_callback(notice_callb)
        await con.execute(
            "DO $$ BEGIN RAISE NOTICE 'catch me!'; END; $$ LANGUAGE plpgsql"
        )
        await con.execute(
            "DO $$ BEGIN RAISE WARNING 'catch me!'; END; $$ LANGUAGE plpgsql"
        )

        expect_msg = {
            'context': 'PL/pgSQL function inline_code_block line 1 at RAISE',
            'message': 'catch me!',
            'server_source_filename': 'pl_exec.c',
            'server_source_function': 'exec_stmt_raise',
            'server_source_line': '***'}

        expect_msg_notice = expect_msg.copy()
        expect_msg_notice.update({
            'severity': 'NOTICE',
            'severity_en': 'NOTICE',
            'sqlstate': '00000',
        })

        expect_msg_warn = expect_msg.copy()
        expect_msg_warn.update({
            'severity': 'WARNING',
            'severity_en': 'WARNING',
            'sqlstate': '01000',
        })

        if con.get_server_version() < (9, 6):
            del expect_msg_notice['context']
            del expect_msg_notice['severity_en']
            del expect_msg_warn['context']
            del expect_msg_warn['severity_en']

        self.assertEqual(
            await q1.get(),
            (con, PostgresNotice, expect_msg_notice))

        self.assertEqual(
            await q1.get(),
            (con, PostgresWarning, expect_msg_warn))

        con.remove_notice_callback(notice_callb)
        await con.execute(
            "DO $$ BEGIN RAISE NOTICE '/dev/null!'; END; $$ LANGUAGE plpgsql"
        )

        self.assertTrue(q1.empty())


    async def test_notify_sequence(self):
        q1 = asyncio.Queue(loop=self.loop)

        cur_id = None

        def notice_callb(con, message):
            q1.put_nowait((con, cur_id, message.message))

        con = self.con
        await con.execute(
            "CREATE FUNCTION _test(i INT) RETURNS int LANGUAGE plpgsql AS $$"
            " BEGIN"
            " RAISE NOTICE '1_%', i;"
            " PERFORM pg_sleep(0.1);"
            " RAISE NOTICE '2_%', i;"
            " RETURN i;"
            " END"
            "$$"
        )
        con.add_notice_callback(notice_callb)
        for cur_id in range(10):
            await con.execute("SELECT _test($1)", cur_id)

        for cur_id in range(10):
            self.assertEqual(
                q1.get_nowait(),
                (con, cur_id, '1_%s' % cur_id))
            self.assertEqual(
                q1.get_nowait(),
                (con, cur_id, '2_%s' % cur_id))

        con.remove_notice_callback(notice_callb)
        self.assertTrue(q1.empty())
