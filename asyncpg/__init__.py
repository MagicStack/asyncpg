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
        self._types_stmt = None

    def get_settings(self):
        return self._protocol.get_settings()

    async def query(self, query):
        waiter = _create_future(self._loop)
        self._protocol.query(query, waiter)
        return await waiter

    async def prepare(self, query):
        state = await self._protocol.prepare(None, query)
        while True:
            ready = state._init_types()
            if ready is True:
                break
            if self._types_stmt is None:
                self._types_stmt = await self.prepare(INTRO_LOOKUP_TYPE)

            types = await self._types_stmt.execute(list(ready))
            self._protocol._add_types(types)
            break
        return PreparedStatement(self, state)

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


INTRO_LOOKUP_TYPE = '''\
SELECT
    bt.oid,
    ns.nspname as namespace,
    bt.typname,
    bt.typtype,
    bt.typlen,
    bt.typelem,
    bt.typrelid,
    ae.oid AS ae_typid,
    ae.typreceive::oid != 0 AS ae_hasbin_input,
    ae.typsend::oid != 0 AS ae_hasbin_output

FROM pg_catalog.pg_type bt
    LEFT JOIN pg_type ae ON (
        bt.typlen = -1 AND
        bt.typelem != 0 AND
        bt.typelem = ae.oid
    )
    LEFT JOIN pg_catalog.pg_namespace ns ON (
        ns.oid = bt.typnamespace)
WHERE
    bt.oid = any($1::oid[]);
'''
