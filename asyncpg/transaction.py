import enum

from . import exceptions as apg_errors


class TransactionState(enum.Enum):
    NEW = 0
    STARTED = 1
    COMMITTED = 2
    ROLLEDBACK = 3
    FAILED = 4


ISOLATION_LEVELS = {'read_committed', 'serializable', 'repeatable_read'}


class Transaction:

    __slots__ = ('_connection', '_isolation', '_readonly', '_deferrable',
                 '_state', '_nested', '_id', '_managed')

    def __init__(self, connection, isolation, readonly, deferrable):
        if isolation not in ISOLATION_LEVELS:
            raise ValueError(
                'isolation is expected to be either of {}, '
                'got {!r}'.format(ISOLATION_LEVELS, isolation))

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
        self._managed = False

    async def __aenter__(self):
        if self._managed:
            raise apg_errors.InterfaceError(
                'cannot enter context: already in an `async def` block')
        self._managed = True
        await self.start()

    async def __aexit__(self, extype, ex, tb):
        try:
            if extype is not None:
                await self.__rollback()
            else:
                await self.__commit()
        finally:
            self._managed = False

    async def start(self):
        self.__check_state_base('start')
        if self._state is TransactionState.STARTED:
            raise apg_errors.InterfaceError(
                'cannot start; the transaction is already started')

        con = self._connection

        if con._top_xact is None:
            con._top_xact = self
        else:
            # Nested transaction block
            top_xact = con._top_xact
            if self._isolation != top_xact._isolation:
                raise apg_errors.InterfaceError(
                    'nested transaction has a different isolation level: '
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
            await self._connection.execute(query)
        except:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    def __check_state_base(self, opname):
        if self._state is TransactionState.COMMITTED:
            raise apg_errors.InterfaceError(
                'cannot {}; the transaction is already committed'.format(
                    opname))
        if self._state is TransactionState.ROLLEDBACK:
            raise apg_errors.InterfaceError(
                'cannot {}; the transaction is already rolled back'.format(
                    opname))
        if self._state is TransactionState.FAILED:
            raise apg_errors.InterfaceError(
                'cannot {}; the transaction is in error state'.format(
                    opname))

    def __check_state(self, opname):
        if self._state is not TransactionState.STARTED:
            if self._state is TransactionState.NEW:
                raise apg_errors.InterfaceError(
                    'cannot {}; the transaction is not yet started'.format(
                        opname))
            self.__check_state_base(opname)

    async def __commit(self):
        self.__check_state('commit')

        if self._connection._top_xact is self:
            self._connection._top_xact = None

        if self._nested:
            query = 'RELEASE SAVEPOINT {};'.format(self._id)
        else:
            query = 'COMMIT;'

        try:
            await self._connection.execute(query)
        except:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.COMMITTED

    async def __rollback(self):
        self.__check_state('rollback')

        if self._connection._top_xact is self:
            self._connection._top_xact = None

        if self._nested:
            query = 'ROLLBACK TO {};'.format(self._id)
        else:
            query = 'ROLLBACK;'

        try:
            await self._connection.execute(query)
        except:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.ROLLEDBACK

    async def commit(self):
        if self._managed:
            raise apg_errors.InterfaceError(
                'cannot manually commit from within an `async def` block')
        await self.__commit()

    async def rollback(self):
        if self._managed:
            raise apg_errors.InterfaceError(
                'cannot manually rollback from within an `async def` block')
        await self.__rollback()

    def __repr__(self):
        attrs = []
        attrs.append('state:{}'.format(self._state.name.lower()))
        attrs.append(self._isolation)
        if self._readonly:
            attrs.append('readonly')
        if self._deferrable:
            attrs.append('deferrable')
        return '<asyncpg.Transaction {} {:#x}>'.format(
            ' '.join(attrs), id(self))
