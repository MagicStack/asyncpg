import asyncpg

from asyncpg import _testbase as tb


class TestConnect(tb.ConnectedTestCase):

    async def test_connect_1(self):
        with self.assertRaisesRegex(
                Exception, 'role "__does_not_exist__" does not exist'):
            await asyncpg.connect(user="__does_not_exist__", loop=self.loop)
