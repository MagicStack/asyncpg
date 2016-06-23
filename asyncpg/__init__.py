import asyncio
import getpass
import os


from .exceptions import *
from .protocol import Protocol


__all__ = ('connect',) + exceptions.__all__


class Connection:
    def __init__(self, protocol, transport, loop):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop

    def get_settings(self):
        return self._protocol.get_settings()

    async def query(self, query):
        waiter = _create_future(self._loop)
        self._protocol.query(query, waiter)
        return await waiter

    async def prepare(self, query):
        waiter = _create_future(self._loop)
        self._protocol.prepare(None, query, waiter)
        state = await waiter
        return PreparedStatement(self, state)

    def close(self):
        self._transport.close()


class PreparedStatement:
    def __init__(self, connection, state):
        self._connection = connection
        self._state = state

    async def execute(self, *args):
        protocol = self._connection._protocol
        waiter = _create_future(self._connection._loop)
        protocol.execute(self._state, args, waiter)
        return await waiter


async def connect(iri=None, *,
                  host=None, port=None,
                  user=None, password=None,
                  dbname=None,
                  loop=None,
                  timeout=60):

    if loop is None:
        loop = asyncio.get_event_loop()

    # On env-var -> connection parameter conversion read here:
    # https://www.postgresql.org/docs/current/static/libpq-envars.html

    if host is None:
        host = os.getenv('PGHOST')
        if host is None:
            host = ['/tmp', '/private/tmp',
                    '/var/pgsql_socket', '/run/postgresql',
                    'localhost']
    if not isinstance(host, list):
        host = [host]

    if port is None:
        port = os.getenv('PGPORT')
        if port is None:
            port = 5432

    if user is None:
        user = os.getenv('PGUSER')
        if user is None:
            user = getpass.getuser()

    if password is None:
        password = os.getenv('PGPASSWORD')

    if dbname is None:
        dbname = os.getenv('PGDATABASE')

    connected = _create_future(loop)
    last_ex = None
    proto_factory = lambda: Protocol(connected, user, password, dbname, loop)

    for h in host:
        if h.startswith('/'):
            # UNIX socket name
            sname = os.path.join(h, '.s.PGSQL.{}'.format(port))
            conn = loop.create_unix_connection(proto_factory, sname)
        else:
            conn = loop.create_connection(proto_factory, h, port)

        try:
            tr, pr = await asyncio.wait_for(conn, timeout=timeout, loop=loop)
        except (OSError, asyncio.TimeoutError) as ex:
            last_ex = ex
        else:
            break
    else:
        raise last_ex

    await connected
    return Connection(pr, tr, loop)


def _create_future(loop):
    try:
        create_future = loop.create_future
    except AttributeError:
        return asyncio.Future(loop=loop)
    else:
        return create_future()
