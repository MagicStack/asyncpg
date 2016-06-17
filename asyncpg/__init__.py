import asyncio

from .protocol import Protocol


__all__ = ('connect',)


class Connection:
    def __init__(self, protocol, transport, loop):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop

    async def query(self, query):
        waiter = asyncio.Future(loop=self._loop)
        self._protocol.query(query, waiter)
        return await waiter

    async def prepare(self, name, query):
        waiter = asyncio.Future(loop=self._loop)
        self._protocol.prepare(name, query, waiter)
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
