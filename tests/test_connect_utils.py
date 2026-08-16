import asyncio
import ssl
import unittest

from asyncpg import connect_utils, exceptions


class TLSUpgradeProtoTests(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.addCleanup(self.loop.close)

    def make_proto(self):
        context = ssl.create_default_context()
        return connect_utils.TLSUpgradeProto(
            self.loop,
            'localhost',
            5432,
            context,
            False,
        )

    def test_server_error_response_is_preserved(self):
        proto = self.make_proto()
        proto.data_received(
            b'Ecould not fork new process for connection: Cannot allocate memory\n\x00'
        )

        with self.assertRaisesRegex(
            exceptions.InterfaceError,
            'could not fork new process for connection: Cannot allocate memory',
        ):
            self.loop.run_until_complete(proto.on_data)
