import asyncio
import getpass
import os


from . import introspection as _intro
from .exceptions import *
from .protocol import Protocol


__all__ = ('connect',) + exceptions.__all__


class Connection:
    def __init__(self, protocol, transport, loop):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop
        self._types_stmt = None
        self._type_by_name_stmt = None

    def get_settings(self):
        return self._protocol.get_settings()

    async def query(self, query):
        waiter = _create_future(self._loop)
        self._protocol.query(query, waiter)
        return await waiter

    async def prepare(self, query):
        state = await self._protocol.prepare(None, query)

        ready = state._init_types()
        if ready is not True:
            if self._types_stmt is None:
                self._types_stmt = await self.prepare(
                    _intro.INTRO_LOOKUP_TYPES)

            types = await self._types_stmt.execute(list(ready))
            self._protocol._add_types(types)

        return PreparedStatement(self, state)

    async def set_type_codec(self, typename, *,
                             schema='public', encoder, decoder,
                             format=1):
        if self._type_by_name_stmt is None:
            self._type_by_name_stmt = await self.prepare(_intro.TYPE_BY_NAME)

        typeoid = await self._type_by_name_stmt.execute(typename, schema)
        if not typeoid:
            raise ValueError('unknown type: {}.{}'.format(schema, typename))

        typeoid = list(typeoid)[0][0]

        self._protocol._add_python_codec(typeoid, encoder, decoder, format)

    def close(self):
        self._transport.close()


class PreparedStatement:
    def __init__(self, connection, state):
        self._connection = connection
        self._state = state

    async def execute(self, *args):
        protocol = self._connection._protocol
        return await protocol.execute(self._state, args)


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

    last_ex = None
    for h in host:
        connected = _create_future(loop)

        if h.startswith('/'):
            # UNIX socket name
            sname = os.path.join(h, '.s.PGSQL.{}'.format(port))
            conn = loop.create_unix_connection(
                lambda: Protocol(sname, connected, user,
                                 password, dbname, loop),
                sname)
        else:
            conn = loop.create_connection(
                lambda: Protocol((h, port), connected, user,
                                 password, dbname, loop),
                h, port)

        try:
            tr, pr = await asyncio.wait_for(conn, timeout=timeout, loop=loop)
        except (OSError, asyncio.TimeoutError) as ex:
            last_ex = ex
            tr.close()
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
