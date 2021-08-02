# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import os
import platform
import sys
import unittest

from asyncpg import _testbase as tb
from asyncpg import exceptions


class TestListeners(tb.ClusterTestCase):

    async def test_listen_01(self):
        async with self.create_pool(database='postgres') as pool:
            async with pool.acquire() as con:

                q1 = asyncio.Queue()
                q2 = asyncio.Queue()

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
                    await asyncio.wait_for(q2.get(), timeout=0.05)

                await con.reset()
                await con.remove_listener('test', listener1)
                await con.execute("NOTIFY test, 'aaaa'")

                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(q1.get(), timeout=0.05)
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(q2.get(), timeout=0.05)

    async def test_listen_02(self):
        async with self.create_pool(database='postgres') as pool:
            async with pool.acquire() as con1, pool.acquire() as con2:

                q1 = asyncio.Queue()

                def listener1(*args):
                    q1.put_nowait(args)

                await con1.add_listener('ipc', listener1)
                await con2.execute("NOTIFY ipc, 'hello'")

                self.assertEqual(
                    await q1.get(),
                    (con1, con2.get_server_pid(), 'ipc', 'hello'))

                await con1.remove_listener('ipc', listener1)

    async def test_listen_notletters(self):
        async with self.create_pool(database='postgres') as pool:
            async with pool.acquire() as con1, pool.acquire() as con2:

                q1 = asyncio.Queue()

                def listener1(*args):
                    q1.put_nowait(args)

                await con1.add_listener('12+"34', listener1)
                await con2.execute("""NOTIFY "12+""34", 'hello'""")

                self.assertEqual(
                    await q1.get(),
                    (con1, con2.get_server_pid(), '12+"34', 'hello'))

                await con1.remove_listener('12+"34', listener1)

    async def test_dangling_listener_warns(self):
        async with self.create_pool(database='postgres') as pool:
            with self.assertWarnsRegex(
                    exceptions.InterfaceWarning,
                    '.*Connection.*is being released to the pool but '
                    'has 1 active notification listener'):
                async with pool.acquire() as con:
                    def listener1(*args):
                        pass

                    await con.add_listener('ipc', listener1)


class TestLogListeners(tb.ConnectedTestCase):

    @tb.with_connection_options(server_settings={
        'client_min_messages': 'notice'
    })
    async def test_log_listener_01(self):
        q1 = asyncio.Queue()

        def notice_callb(con, message):
            # Message fields depend on PG version, hide some values.
            dct = message.as_dict()
            del dct['server_source_line']
            q1.put_nowait((con, type(message), dct))

        async def raise_notice():
            await self.con.execute(
                """DO $$
                    BEGIN RAISE NOTICE 'catch me!'; END;
                $$ LANGUAGE plpgsql"""
            )

        async def raise_warning():
            await self.con.execute(
                """DO $$
                    BEGIN RAISE WARNING 'catch me!'; END;
                $$ LANGUAGE plpgsql"""
            )

        con = self.con
        con.add_log_listener(notice_callb)

        expected_msg = {
            'context': 'PL/pgSQL function inline_code_block line 2 at RAISE',
            'message': 'catch me!',
            'server_source_function': 'exec_stmt_raise',
        }

        expected_msg_notice = {
            **expected_msg,
            'severity': 'NOTICE',
            'severity_en': 'NOTICE',
            'sqlstate': '00000',
        }

        expected_msg_warn = {
            **expected_msg,
            'severity': 'WARNING',
            'severity_en': 'WARNING',
            'sqlstate': '01000',
        }

        if con.get_server_version() < (9, 6):
            del expected_msg_notice['context']
            del expected_msg_notice['severity_en']
            del expected_msg_warn['context']
            del expected_msg_warn['severity_en']

        await raise_notice()
        await raise_warning()

        msg = await q1.get()
        msg[2].pop('server_source_filename', None)
        self.assertEqual(
            msg,
            (con, exceptions.PostgresLogMessage, expected_msg_notice))

        msg = await q1.get()
        msg[2].pop('server_source_filename', None)
        self.assertEqual(
            msg,
            (con, exceptions.PostgresWarning, expected_msg_warn))

        con.remove_log_listener(notice_callb)
        await raise_notice()
        self.assertTrue(q1.empty())

        con.add_log_listener(notice_callb)
        await raise_notice()
        await q1.get()
        self.assertTrue(q1.empty())
        await con.reset()
        await raise_notice()
        self.assertTrue(q1.empty())

    @tb.with_connection_options(server_settings={
        'client_min_messages': 'notice'
    })
    async def test_log_listener_02(self):
        q1 = asyncio.Queue()

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

        try:
            con.add_log_listener(notice_callb)
            for cur_id in range(10):
                await con.execute("SELECT _test($1)", cur_id)

            for cur_id in range(10):
                self.assertEqual(
                    q1.get_nowait(),
                    (con, cur_id, '1_%s' % cur_id))
                self.assertEqual(
                    q1.get_nowait(),
                    (con, cur_id, '2_%s' % cur_id))

            con.remove_log_listener(notice_callb)
            self.assertTrue(q1.empty())
        finally:
            await con.execute('DROP FUNCTION _test(i INT)')

    @tb.with_connection_options(server_settings={
        'client_min_messages': 'notice'
    })
    async def test_log_listener_03(self):
        q1 = asyncio.Queue()

        async def raise_message(level, code):
            await self.con.execute("""
                DO $$ BEGIN
                    RAISE {} 'catch me!' USING ERRCODE = '{}';
                END; $$ LANGUAGE plpgsql;
            """.format(level, code))

        def notice_callb(con, message):
            # Message fields depend on PG version, hide some values.
            q1.put_nowait(message)

        self.con.add_log_listener(notice_callb)

        await raise_message('WARNING', '99999')
        msg = await q1.get()
        self.assertIsInstance(msg, exceptions.PostgresWarning)
        self.assertEqual(msg.sqlstate, '99999')

        await raise_message('WARNING', '01004')
        msg = await q1.get()
        self.assertIsInstance(msg, exceptions.StringDataRightTruncation)
        self.assertEqual(msg.sqlstate, '01004')

        with self.assertRaises(exceptions.InvalidCharacterValueForCastError):
            await raise_message('', '22018')
        self.assertTrue(q1.empty())

    async def test_dangling_log_listener_warns(self):
        async with self.create_pool(database='postgres') as pool:
            with self.assertWarnsRegex(
                    exceptions.InterfaceWarning,
                    '.*Connection.*is being released to the pool but '
                    'has 1 active log listener'):
                async with pool.acquire() as con:
                    def listener1(*args):
                        pass

                    con.add_log_listener(listener1)


@unittest.skipIf(os.environ.get('PGHOST'), 'using remote cluster for testing')
@unittest.skipIf(
    platform.system() == 'Windows' and
    sys.version_info >= (3, 8),
    'not compatible with ProactorEventLoop which is default in Python 3.8')
class TestConnectionTerminationListener(tb.ProxiedClusterTestCase):

    async def test_connection_termination_callback_called_on_remote(self):

        called = False

        def close_cb(con):
            nonlocal called
            called = True

        con = await self.connect()
        con.add_termination_listener(close_cb)
        self.proxy.close_all_connections()
        try:
            await con.fetchval('SELECT 1')
        except Exception:
            pass
        self.assertTrue(called)

    async def test_connection_termination_callback_called_on_local(self):

        called = False

        def close_cb(con):
            nonlocal called
            called = True

        con = await self.connect()
        con.add_termination_listener(close_cb)
        await con.close()
        self.assertTrue(called)
