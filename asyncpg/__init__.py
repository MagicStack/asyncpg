import asyncio
import enum
import getpass
import os


from . import introspection as _intro
from .exceptions import *
from .protocol import Protocol


__all__ = ('connect',) + exceptions.__all__


class Connection:

    __slots__ = ('_protocol', '_transport', '_loop', '_types_stmt',
                 '_type_by_name_stmt', '_top_xact', '_uid')

    def __init__(self, protocol, transport, loop):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop
        self._types_stmt = None
        self._type_by_name_stmt = None
        self._top_xact = None
        self._uid = 0

    def get_settings(self):
        return self._protocol.get_settings()

    def transaction(self, *, isolation='read_committed', readonly=False,
                    deferrable=False):

        return Transaction(self, isolation, readonly, deferrable)

    async def execute_script(self, script):
        await self._protocol.query(script)

    async def execute(self, query, *args):
        stmt = await self._prepare('', query)
        return await stmt.execute(*args)

    async def prepare(self, query):
        return await self._prepare(None, query)

    async def _prepare(self, name, query):
        state = await self._protocol.prepare(name, query)

        ready = state._init_types()
        if ready is not True:
            if self._types_stmt is None:
                self._types_stmt = await self.prepare(
                    _intro.INTRO_LOOKUP_TYPES)

            types = await self._types_stmt.execute(list(ready))
            self._protocol._add_types(types)

        return PreparedStatement(self, state)

    async def set_type_codec(self, typename, *,
                             schema='public', encoder, decoder, binary=False):
        """Set an encoder/decoder pair for the specified data type

        :param typename:  Name of the data type the codec is for.
        :param schema:  Schema name of the data type the codec is for
                        (defaults to 'public')
        :param encoder:  Callable accepting a single argument and returning
                         a string or a bytes object (if `binary` is True).
        :param decoder:  Callable accepting a single string or bytes argument
                         and returning a decoded object.
        :param binary:  Specifies whether the codec is able to handle binary
                        data.  If ``False`` (the default), the data is
                        expected to be encoded/decoded in text.
        """
        if self._type_by_name_stmt is None:
            self._type_by_name_stmt = await self.prepare(_intro.TYPE_BY_NAME)

        typeinfo = await self._type_by_name_stmt.execute(typename, schema)
        if not typeinfo:
            raise ValueError('unknown type: {}.{}'.format(schema, typename))
        typeinfo = list(typeinfo)[0]

        oid = typeinfo['oid']
        if typeinfo['kind'] != b'b' or typeinfo['elemtype']:
            raise ValueError(
                'cannot use custom codec on non-scalar type {}.{}'.format(
                    schema, typename))

        self._protocol._add_python_codec(
            oid, typename, schema, 'scalar',
            encoder, decoder, binary)

    def close(self):
        self._transport.close()

    def _get_unique_id(self):
        self._uid += 1
        return 'id{}'.format(self._uid)


class PreparedStatement:

    __slots__ = ('_connection', '_state')

    def __init__(self, connection, state):
        self._connection = connection
        self._state = state

    def get_parameters(self):
        return self._state._get_parameters()

    def get_attributes(self):
        return self._state._get_attributes()

    async def execute(self, *args):
        protocol = self._connection._protocol
        return await protocol.execute(self._state, args)


class TransactionState(enum.Enum):
    NEW             = 0
    STARTED         = 1
    COMMITTED       = 2
    ROLLEDBACK      = 3
    FAILED          = 4


class Transaction:

    ISOLATION_LEVELS = {'read_committed', 'serializable', 'repeatable_read'}

    __slots__ = ('_connection', '_isolation', '_readonly', '_deferrable',
                 '_state', '_nested', '_id')

    def __init__(self, connection, isolation, readonly, deferrable):
        if isolation not in self.ISOLATION_LEVELS:
            raise ValueError(
                'isolation is expected to be either of {}, '
                'got {!r}'.format(self.ISOLATION_LEVELS, isolation))

        if isolation != 'serializable':
            if readonly:
                raise ValueError(
                    '"readonly" is only supported for '
                    'serializable transactions')

            if deferrable and not readonly:
                raise ValueError(
                    '"deferrable" is only supported for '
                    'serializable readonly transactions')

        self._connection = connection
        self._isolation = isolation
        self._readonly = readonly
        self._deferrable = deferrable
        self._state = TransactionState.NEW
        self._nested = False
        self._id = None

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, extype, ex, tb):
        if extype is not None:
            await self.rollback()

    async def start(self):
        if self._state is not TransactionState.NEW:
            raise FatalError('cannot start transaction: inconsistent state')

        con = self._connection

        if con._top_xact is None:
            con._top_xact = self
        else:
            # Nested transaction block
            top_xact = con._top_xact
            if self._isolation != top_xact._isolation:
                raise FatalError(
                    'nested transaction has different isolation level: '
                    'current {!r} != outer {!r}'.format(
                        self._isolation, top_xact._isolation))
            self._nested = True

        if self._nested:
            self._id = con._get_unique_id()
            query = 'SAVEPOINT {};'.format(self._id)
        else:
            if self._isolation == 'read_committed':
                query = 'BEGIN;'
            elif self._isolation == 'repeatable_read':
                query = 'BEGIN ISOLATION LEVEL REPEATABLE READ;'
            else:
                query = 'BEGIN ISOLATION LEVEL SERIALIZABLE'
                if self._readonly:
                    query += ' READ ONLY'
                if self._deferrable:
                    query += ' DEFERRABLE'
                query += ';'

        try:
            await self._connection.execute_script(query)
        except:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    async def commit(self):
        if self._state is not TransactionState.STARTED:
            raise FatalError('cannot commit transaction: inconsistent state')

        if self._nested:
            query = 'RELEASE SAVEPOINT {};'.format(self._id)
        else:
            query = 'COMMIT;'

        try:
            await self._connection.execute_script(query)
        except:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.COMMITTED

    async def rollback(self):
        if self._connection._top_xact is self:
            self._connection._top_xact = None

        if self._state is not TransactionState.STARTED:
            raise FatalError('cannot rollback transaction: inconsistent state')

        if self._nested:
            query = 'ROLLBACK TO {};'.format(self._id)
        else:
            query = 'ROLLBACK;'

        try:
            await self._connection.execute_script(query)
        except:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.ROLLEDBACK


async def connect(iri=None, *,
                  host=None, port=None,
                  user=None, password=None,
                  database=None,
                  loop=None,
                  timeout=60):

    if loop is None:
        loop = asyncio.get_event_loop()

    # On env-var -> connection parameter conversion read here:
    # https://www.postgresql.org/docs/current/static/libpq-envars.html
    # Note that env values may be an empty string in cases when
    # the variable is "unset" by setting it to an empty value
    #
    if host is None:
        host = os.getenv('PGHOST')
        if not host:
            host = ['/tmp', '/private/tmp',
                    '/var/pgsql_socket', '/run/postgresql',
                    'localhost']
    if not isinstance(host, list):
        host = [host]

    if port is None:
        port = os.getenv('PGPORT')
        if not port:
            port = 5432

    if user is None:
        user = os.getenv('PGUSER')
        if not user:
            user = getpass.getuser()

    if password is None:
        password = os.getenv('PGPASSWORD')

    if database is None:
        database = os.getenv('PGDATABASE')

    last_ex = None
    for h in host:
        connected = _create_future(loop)

        if h.startswith('/'):
            # UNIX socket name
            sname = os.path.join(h, '.s.PGSQL.{}'.format(port))
            conn = loop.create_unix_connection(
                lambda: Protocol(sname, connected, user,
                                 password, database, loop),
                sname)
        else:
            conn = loop.create_connection(
                lambda: Protocol((h, port), connected, user,
                                 password, database, loop),
                h, port)

        try:
            tr, pr = await asyncio.wait_for(conn, timeout=timeout, loop=loop)
        except (OSError, asyncio.TimeoutError) as ex:
            last_ex = ex
        else:
            break
    else:
        raise last_ex

    try:
        await connected
    except:
        tr.close()
        raise

    return Connection(pr, tr, loop)


def _create_future(loop):
    try:
        create_future = loop.create_future
    except AttributeError:
        return asyncio.Future(loop=loop)
    else:
        return create_future()
