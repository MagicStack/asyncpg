import asyncio

from .protocol import Protocol


__all__ = ('connect',)


class Connection:
    def __init__(self, protocol, transport, loop):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop

    def get_settings(self):
        return self._protocol.get_settings()

    async def query(self, query):
        waiter = asyncio.Future(loop=self._loop)
        self._protocol.query(query, waiter)
        return await waiter

    async def prepare(self, query):
        waiter = asyncio.Future(loop=self._loop)
        self._protocol.prepare(None, query, waiter)
        state = await waiter
        return PreparedStatement(self, state)


class PreparedStatement:
    def __init__(self, connection, state):
        self._connection = connection
        self._state = state

    async def execute(self, *args):
        protocol = self._connection._protocol
        waiter = asyncio.Future(loop=self._connection._loop)
        protocol.execute(self._state, args, waiter)
        return await waiter


async def connect(host='localhost', port=5432, user='postgres', *,
                  loop=None):

    if loop is None:
        loop = asyncio.get_event_loop()

    connected = asyncio.Future(loop=loop)

    tr, pr = await loop.create_connection(
        lambda: Protocol(connected, user, loop),
        host, port)

    await connected

    return Connection(pr, tr, loop)
