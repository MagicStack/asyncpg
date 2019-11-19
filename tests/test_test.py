# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import types
import unittest


from asyncpg import _testbase as tb


class BaseSimpleTestCase:

    async def test_tests_zero_error(self):
        await asyncio.sleep(0.01)
        1 / 0


class TestTests(unittest.TestCase):

    def test_tests_fail_1(self):
        SimpleTestCase = types.new_class('SimpleTestCase',
                                         (BaseSimpleTestCase, tb.TestCase))

        suite = unittest.TestSuite()
        suite.addTest(SimpleTestCase('test_tests_zero_error'))

        result = unittest.TestResult()
        suite.run(result)

        self.assertIn('ZeroDivisionError', result.errors[0][1])


class TestHelpers(tb.TestCase):

    async def test_tests_assertLoopErrorHandlerCalled_01(self):
        with self.assertRaisesRegex(AssertionError, r'no message.*was logged'):
            with self.assertLoopErrorHandlerCalled('aa'):
                self.loop.call_exception_handler({'message': 'bb a bb'})

        with self.assertLoopErrorHandlerCalled('aa'):
            self.loop.call_exception_handler({'message': 'bbaabb'})
