import asyncio
import collections
import socket
import struct

from . import cursor
from . import introspection
from . import prepared_stmt
from . import transaction


class Connection:

    __slots__ = ('_protocol', '_transport', '_loop', '_types_stmt',
                 '_type_by_name_stmt', '_top_xact', '_uid', '_aborted',
                 '_stmt_cache_max_size', '_stmt_cache', '_stmts_to_close',
                 '_addr', '_opts', '_command_timeout')

    def __init__(self, protocol, transport, loop, addr, opts, *,
                 statement_cache_size, command_timeout):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop
        self._types_stmt = None
        self._type_by_name_stmt = None
        self._top_xact = None
        self._uid = 0
        self._aborted = False

        self._addr = addr
        self._opts = opts

        self._stmt_cache_max_size = statement_cache_size
        self._stmt_cache = collections.OrderedDict()
        self._stmts_to_close = set()

        self._command_timeout = command_timeout

    def get_settings(self):
        return self._protocol.get_settings()

    def transaction(self, *, isolation='read_committed', readonly=False,
                    deferrable=False):

        return transaction.Transaction(self, isolation, readonly, deferrable)

    async def execute(self, script: str, *, timeout: float=None) -> str:
        """Execute an SQL command (or commands).

        :return str: Status of the last SQL command.
        """
        return await self._protocol.query(script, timeout)

    async def _get_statement(self, query, timeout):
        cache = self._stmt_cache_max_size > 0

        if cache:
            try:
                state = self._stmt_cache[query]
            except KeyError:
                pass
            else:
                self._stmt_cache.move_to_end(query, last=True)
                if not state.closed:
                    return state

        protocol = self._protocol
        state = await protocol.prepare(None, query, timeout)

        ready = state._init_types()
        if ready is not True:
            if self._types_stmt is None:
                self._types_stmt = await self.prepare(
                    introspection.INTRO_LOOKUP_TYPES)

            types = await self._types_stmt.fetch(list(ready))
            protocol.get_settings().register_data_types(types)

        if cache:
            if len(self._stmt_cache) > self._stmt_cache_max_size - 1:
                old_query, old_state = self._stmt_cache.popitem(last=False)
                self._maybe_gc_stmt(old_state)
            self._stmt_cache[query] = state

        # If we've just created a new statement object, check if there
        # are any statements for GC.
        if self._stmts_to_close:
            await self._cleanup_stmts()

        return state

    def cursor(self, query, *args, prefetch=None, timeout=None):
        return cursor.CursorFactory(self, query, None, args, prefetch, timeout)

    async def prepare(self, query, *, timeout=None):
        stmt = await self._get_statement(query, timeout)
        return prepared_stmt.PreparedStatement(self, query, stmt)

    async def fetch(self, query, *args, timeout=None):
        stmt = await self._get_statement(query, timeout)
        protocol = self._protocol
        data = await protocol.bind_execute(stmt, args, '', 0, False, timeout)
        return data

    async def fetchval(self, query, *args, column=0, timeout=None):
        stmt = await self._get_statement(query, timeout)
        protocol = self._protocol
        data = await protocol.bind_execute(stmt, args, '', 1, False, timeout)
        if not data:
            return None
        return data[0][column]

    async def fetchrow(self, query, *args, timeout=None):
        stmt = await self._get_statement(query, timeout)
        protocol = self._protocol
        data = await protocol.bind_execute(stmt, args, '', 1, False, timeout)
        if not data:
            return None
        return data[0]

    async def set_type_codec(self, typename, *,
                             schema='public', encoder, decoder, binary=False):
        """Set an encoder/decoder pair for the specified data type.

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
            self._type_by_name_stmt = await self.prepare(
                introspection.TYPE_BY_NAME)

        typeinfo = await self._type_by_name_stmt.fetchrow(
            typename, schema)
        if not typeinfo:
            raise ValueError('unknown type: {}.{}'.format(schema, typename))

        oid = typeinfo['oid']
        if typeinfo['kind'] != b'b' or typeinfo['elemtype']:
            raise ValueError(
                'cannot use custom codec on non-scalar type {}.{}'.format(
                    schema, typename))

        self._protocol.get_settings().add_python_codec(
            oid, typename, schema, 'scalar',
            encoder, decoder, binary)

    async def set_builtin_type_codec(self, typename, *,
                                     schema='public', codec_name):
        """Set a builtin codec for the specified data type.

        :param typename:  Name of the data type the codec is for.
        :param schema:  Schema name of the data type the codec is for
                        (defaults to 'public')
        :param codec_name:  The name of the builtin codec.
        """
        if self._type_by_name_stmt is None:
            self._type_by_name_stmt = await self.prepare(
                introspection.TYPE_BY_NAME)

        typeinfo = await self._type_by_name_stmt.fetchrow(
            typename, schema)
        if not typeinfo:
            raise ValueError('unknown type: {}.{}'.format(schema, typename))

        oid = typeinfo['oid']
        if typeinfo['kind'] != b'b' or typeinfo['elemtype']:
            raise ValueError(
                'cannot alias non-scalar type {}.{}'.format(
                    schema, typename))

        self._protocol.get_settings().set_builtin_type_codec(
            oid, typename, schema, 'scalar', codec_name)

    def is_closed(self):
        return self._protocol.is_closed() or self._aborted

    async def close(self):
        if self.is_closed():
            return
        self._close_stmts()
        self._aborted = True
        protocol = self._protocol
        await protocol.close()

    def terminate(self):
        self._close_stmts()
        self._aborted = True
        self._protocol.abort()

    def _get_unique_id(self):
        self._uid += 1
        return 'id{}'.format(self._uid)

    def _close_stmts(self):
        for stmt in self._stmt_cache.values():
            stmt.mark_closed()

        for stmt in self._stmts_to_close:
            stmt.mark_closed()

        self._stmt_cache.clear()
        self._stmts_to_close.clear()

    def _maybe_gc_stmt(self, stmt):
        if stmt.refs == 0 and stmt.query not in self._stmt_cache:
            stmt.mark_closed()
            self._stmts_to_close.add(stmt)

    async def _cleanup_stmts(self):
        to_close = self._stmts_to_close
        self._stmts_to_close = set()
        protocol = self._protocol
        for stmt in to_close:
            await protocol.close_statement(stmt, False)

    def _request_portal_name(self):
        return self._get_unique_id()

    def _cancel_current_command(self):
        async def cancel():
            if isinstance(self._addr, str):
                r, w = await asyncio.open_unix_connection(
                    self._addr, loop=self._loop)
            else:
                r, w = await asyncio.open_connection(
                    *self._addr, loop=self._loop)

                sock = w.transport.get_extra_info('socket')
                sock.setsockopt(socket.IPPROTO_TCP,
                                socket.TCP_NODELAY, 1)

            # Pack CancelRequest message
            msg = struct.pack('!llll', 16, 80877102,
                              self._protocol.backend_pid,
                              self._protocol.backend_secret)
            w.write(msg)
            w.close()

        self._loop.create_task(cancel())
