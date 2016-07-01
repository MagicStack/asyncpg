import enum

from . import exceptions as apg_errors


class TransactionState(enum.Enum):
    NEW = 0
    STARTED = 1
    COMMITTED = 2
    ROLLEDBACK = 3
    FAILED = 4


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
            raise apg_errors.FatalError(
                'cannot start transaction: inconsistent state')

        con = self._connection

        if con._top_xact is None:
            con._top_xact = self
        else:
            # Nested transaction block
            top_xact = con._top_xact
            if self._isolation != top_xact._isolation:
                raise apg_errors.FatalError(
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
            await self._connection.execute(query)
        except:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    async def commit(self):
        if self._state is not TransactionState.STARTED:
            raise apg_errors.FatalError(
                'cannot commit transaction: inconsistent state')

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

    async def rollback(self):
        if self._connection._top_xact is self:
            self._connection._top_xact = None

        if self._state is not TransactionState.STARTED:
            raise apg_errors.FatalError(
                'cannot rollback transaction: inconsistent state')

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
