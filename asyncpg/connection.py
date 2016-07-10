import collections

from . import introspection
from . import prepared_stmt
from . import transaction


class Connection:

    __slots__ = ('_protocol', '_transport', '_loop', '_types_stmt',
                 '_type_by_name_stmt', '_top_xact', '_uid', '_aborted',
                 '_stmt_cache_max_size', '_stmt_cache', '_stmts_to_close')

    def __init__(self, protocol, transport, loop):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop
        self._types_stmt = None
        self._type_by_name_stmt = None
        self._top_xact = None
        self._uid = 0
        self._aborted = False

        self._stmt_cache_max_size = 100
        self._stmt_cache = collections.OrderedDict()
        self._stmts_to_close = set()

    def get_settings(self):
        return self._protocol.get_settings()

    def transaction(self, *, isolation='read_committed', readonly=False,
                    deferrable=False):

        return transaction.Transaction(self, isolation, readonly, deferrable)

    async def execute(self, script):
        await self._protocol.query(script)

    async def prepare(self, query):
        try:
            state = self._stmt_cache[query]
        except KeyError:
            pass
        else:
            self._stmt_cache.move_to_end(query, last=True)
            if not state.closed:
                return prepared_stmt.PreparedStatement(self, query, state)

        protocol = self._protocol
        state = await protocol.prepare(None, query)

        ready = state._init_types()
        if ready is not True:
            if self._types_stmt is None:
                self._types_stmt = await self.prepare(
                    introspection.INTRO_LOOKUP_TYPES)

            types = await self._types_stmt.get_list(list(ready))
            protocol.get_settings().register_data_types(types)

        if len(self._stmt_cache) > self._stmt_cache_max_size - 1:
            old_query, old_state = self._stmt_cache.popitem(last=False)
            self._stmts_to_close.add(old_state)
            await self._cleanup_stmts()

        self._stmt_cache[query] = state

        return prepared_stmt.PreparedStatement(self, query, state)

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

        typeinfo = await self._type_by_name_stmt.get_first_row(
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

        typeinfo = await self._type_by_name_stmt.get_first_row(
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
        self._transport.abort()
        await self._protocol.close()

    def terminate(self):
        self._close_stmts()
        self._aborted = True
        self._transport.abort()

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

    async def _cleanup_stmts(self):
        removed = None
        for stmt in self._stmts_to_close:
            if stmt.refs == 0:
                await self._protocol.close_statement(stmt)
                if removed is None:
                    removed = set()
                removed.add(stmt)

        if removed is not None:
            self._stmts_to_close -= removed
