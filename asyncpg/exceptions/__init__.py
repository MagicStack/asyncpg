# GENERATED FROM postgresql/src/backend/utils/errcodes.txt
# DO NOT MODIFY, use tools/generate_exceptions.py to update

from __future__ import annotations

import typing
from ._base import *  # NOQA
from . import _base


class PostgresWarning(_base.PostgresLogMessage, Warning):
    sqlstate: typing.ClassVar[str] = '01000'


class DynamicResultSetsReturned(PostgresWarning):
    sqlstate: typing.ClassVar[str] = '0100C'


class ImplicitZeroBitPadding(PostgresWarning):
    sqlstate: typing.ClassVar[str] = '01008'


class NullValueEliminatedInSetFunction(PostgresWarning):
    sqlstate: typing.ClassVar[str] = '01003'


class PrivilegeNotGranted(PostgresWarning):
    sqlstate: typing.ClassVar[str] = '01007'


class PrivilegeNotRevoked(PostgresWarning):
    sqlstate: typing.ClassVar[str] = '01006'


class StringDataRightTruncation(PostgresWarning):
    sqlstate: typing.ClassVar[str] = '01004'


class DeprecatedFeature(PostgresWarning):
    sqlstate: typing.ClassVar[str] = '01P01'


class NoData(PostgresWarning):
    sqlstate: typing.ClassVar[str] = '02000'


class NoAdditionalDynamicResultSetsReturned(NoData):
    sqlstate: typing.ClassVar[str] = '02001'


class SQLStatementNotYetCompleteError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '03000'


class PostgresConnectionError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '08000'


class ConnectionDoesNotExistError(PostgresConnectionError):
    sqlstate: typing.ClassVar[str] = '08003'


class ConnectionFailureError(PostgresConnectionError):
    sqlstate: typing.ClassVar[str] = '08006'


class ClientCannotConnectError(PostgresConnectionError):
    sqlstate: typing.ClassVar[str] = '08001'


class ConnectionRejectionError(PostgresConnectionError):
    sqlstate: typing.ClassVar[str] = '08004'


class TransactionResolutionUnknownError(PostgresConnectionError):
    sqlstate: typing.ClassVar[str] = '08007'


class ProtocolViolationError(PostgresConnectionError):
    sqlstate: typing.ClassVar[str] = '08P01'


class TriggeredActionError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '09000'


class FeatureNotSupportedError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '0A000'


class InvalidCachedStatementError(FeatureNotSupportedError):
    pass


class InvalidTransactionInitiationError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '0B000'


class LocatorError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '0F000'


class InvalidLocatorSpecificationError(LocatorError):
    sqlstate: typing.ClassVar[str] = '0F001'


class InvalidGrantorError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '0L000'


class InvalidGrantOperationError(InvalidGrantorError):
    sqlstate: typing.ClassVar[str] = '0LP01'


class InvalidRoleSpecificationError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '0P000'


class DiagnosticsError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '0Z000'


class StackedDiagnosticsAccessedWithoutActiveHandlerError(DiagnosticsError):
    sqlstate: typing.ClassVar[str] = '0Z002'


class CaseNotFoundError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '20000'


class CardinalityViolationError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '21000'


class DataError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '22000'


class ArraySubscriptError(DataError):
    sqlstate: typing.ClassVar[str] = '2202E'


class CharacterNotInRepertoireError(DataError):
    sqlstate: typing.ClassVar[str] = '22021'


class DatetimeFieldOverflowError(DataError):
    sqlstate: typing.ClassVar[str] = '22008'


class DivisionByZeroError(DataError):
    sqlstate: typing.ClassVar[str] = '22012'


class ErrorInAssignmentError(DataError):
    sqlstate: typing.ClassVar[str] = '22005'


class EscapeCharacterConflictError(DataError):
    sqlstate: typing.ClassVar[str] = '2200B'


class IndicatorOverflowError(DataError):
    sqlstate: typing.ClassVar[str] = '22022'


class IntervalFieldOverflowError(DataError):
    sqlstate: typing.ClassVar[str] = '22015'


class InvalidArgumentForLogarithmError(DataError):
    sqlstate: typing.ClassVar[str] = '2201E'


class InvalidArgumentForNtileFunctionError(DataError):
    sqlstate: typing.ClassVar[str] = '22014'


class InvalidArgumentForNthValueFunctionError(DataError):
    sqlstate: typing.ClassVar[str] = '22016'


class InvalidArgumentForPowerFunctionError(DataError):
    sqlstate: typing.ClassVar[str] = '2201F'


class InvalidArgumentForWidthBucketFunctionError(DataError):
    sqlstate: typing.ClassVar[str] = '2201G'


class InvalidCharacterValueForCastError(DataError):
    sqlstate: typing.ClassVar[str] = '22018'


class InvalidDatetimeFormatError(DataError):
    sqlstate: typing.ClassVar[str] = '22007'


class InvalidEscapeCharacterError(DataError):
    sqlstate: typing.ClassVar[str] = '22019'


class InvalidEscapeOctetError(DataError):
    sqlstate: typing.ClassVar[str] = '2200D'


class InvalidEscapeSequenceError(DataError):
    sqlstate: typing.ClassVar[str] = '22025'


class NonstandardUseOfEscapeCharacterError(DataError):
    sqlstate: typing.ClassVar[str] = '22P06'


class InvalidIndicatorParameterValueError(DataError):
    sqlstate: typing.ClassVar[str] = '22010'


class InvalidParameterValueError(DataError):
    sqlstate: typing.ClassVar[str] = '22023'


class InvalidPrecedingOrFollowingSizeError(DataError):
    sqlstate: typing.ClassVar[str] = '22013'


class InvalidRegularExpressionError(DataError):
    sqlstate: typing.ClassVar[str] = '2201B'


class InvalidRowCountInLimitClauseError(DataError):
    sqlstate: typing.ClassVar[str] = '2201W'


class InvalidRowCountInResultOffsetClauseError(DataError):
    sqlstate: typing.ClassVar[str] = '2201X'


class InvalidTablesampleArgumentError(DataError):
    sqlstate: typing.ClassVar[str] = '2202H'


class InvalidTablesampleRepeatError(DataError):
    sqlstate: typing.ClassVar[str] = '2202G'


class InvalidTimeZoneDisplacementValueError(DataError):
    sqlstate: typing.ClassVar[str] = '22009'


class InvalidUseOfEscapeCharacterError(DataError):
    sqlstate: typing.ClassVar[str] = '2200C'


class MostSpecificTypeMismatchError(DataError):
    sqlstate: typing.ClassVar[str] = '2200G'


class NullValueNotAllowedError(DataError):
    sqlstate: typing.ClassVar[str] = '22004'


class NullValueNoIndicatorParameterError(DataError):
    sqlstate: typing.ClassVar[str] = '22002'


class NumericValueOutOfRangeError(DataError):
    sqlstate: typing.ClassVar[str] = '22003'


class SequenceGeneratorLimitExceededError(DataError):
    sqlstate: typing.ClassVar[str] = '2200H'


class StringDataLengthMismatchError(DataError):
    sqlstate: typing.ClassVar[str] = '22026'


class StringDataRightTruncationError(DataError):
    sqlstate: typing.ClassVar[str] = '22001'


class SubstringError(DataError):
    sqlstate: typing.ClassVar[str] = '22011'


class TrimError(DataError):
    sqlstate: typing.ClassVar[str] = '22027'


class UnterminatedCStringError(DataError):
    sqlstate: typing.ClassVar[str] = '22024'


class ZeroLengthCharacterStringError(DataError):
    sqlstate: typing.ClassVar[str] = '2200F'


class PostgresFloatingPointError(DataError):
    sqlstate: typing.ClassVar[str] = '22P01'


class InvalidTextRepresentationError(DataError):
    sqlstate: typing.ClassVar[str] = '22P02'


class InvalidBinaryRepresentationError(DataError):
    sqlstate: typing.ClassVar[str] = '22P03'


class BadCopyFileFormatError(DataError):
    sqlstate: typing.ClassVar[str] = '22P04'


class UntranslatableCharacterError(DataError):
    sqlstate: typing.ClassVar[str] = '22P05'


class NotAnXmlDocumentError(DataError):
    sqlstate: typing.ClassVar[str] = '2200L'


class InvalidXmlDocumentError(DataError):
    sqlstate: typing.ClassVar[str] = '2200M'


class InvalidXmlContentError(DataError):
    sqlstate: typing.ClassVar[str] = '2200N'


class InvalidXmlCommentError(DataError):
    sqlstate: typing.ClassVar[str] = '2200S'


class InvalidXmlProcessingInstructionError(DataError):
    sqlstate: typing.ClassVar[str] = '2200T'


class DuplicateJsonObjectKeyValueError(DataError):
    sqlstate: typing.ClassVar[str] = '22030'


class InvalidArgumentForSQLJsonDatetimeFunctionError(DataError):
    sqlstate: typing.ClassVar[str] = '22031'


class InvalidJsonTextError(DataError):
    sqlstate: typing.ClassVar[str] = '22032'


class InvalidSQLJsonSubscriptError(DataError):
    sqlstate: typing.ClassVar[str] = '22033'


class MoreThanOneSQLJsonItemError(DataError):
    sqlstate: typing.ClassVar[str] = '22034'


class NoSQLJsonItemError(DataError):
    sqlstate: typing.ClassVar[str] = '22035'


class NonNumericSQLJsonItemError(DataError):
    sqlstate: typing.ClassVar[str] = '22036'


class NonUniqueKeysInAJsonObjectError(DataError):
    sqlstate: typing.ClassVar[str] = '22037'


class SingletonSQLJsonItemRequiredError(DataError):
    sqlstate: typing.ClassVar[str] = '22038'


class SQLJsonArrayNotFoundError(DataError):
    sqlstate: typing.ClassVar[str] = '22039'


class SQLJsonMemberNotFoundError(DataError):
    sqlstate: typing.ClassVar[str] = '2203A'


class SQLJsonNumberNotFoundError(DataError):
    sqlstate: typing.ClassVar[str] = '2203B'


class SQLJsonObjectNotFoundError(DataError):
    sqlstate: typing.ClassVar[str] = '2203C'


class TooManyJsonArrayElementsError(DataError):
    sqlstate: typing.ClassVar[str] = '2203D'


class TooManyJsonObjectMembersError(DataError):
    sqlstate: typing.ClassVar[str] = '2203E'


class SQLJsonScalarRequiredError(DataError):
    sqlstate: typing.ClassVar[str] = '2203F'


class SQLJsonItemCannotBeCastToTargetTypeError(DataError):
    sqlstate: typing.ClassVar[str] = '2203G'


class IntegrityConstraintViolationError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '23000'


class RestrictViolationError(IntegrityConstraintViolationError):
    sqlstate: typing.ClassVar[str] = '23001'


class NotNullViolationError(IntegrityConstraintViolationError):
    sqlstate: typing.ClassVar[str] = '23502'


class ForeignKeyViolationError(IntegrityConstraintViolationError):
    sqlstate: typing.ClassVar[str] = '23503'


class UniqueViolationError(IntegrityConstraintViolationError):
    sqlstate: typing.ClassVar[str] = '23505'


class CheckViolationError(IntegrityConstraintViolationError):
    sqlstate: typing.ClassVar[str] = '23514'


class ExclusionViolationError(IntegrityConstraintViolationError):
    sqlstate: typing.ClassVar[str] = '23P01'


class InvalidCursorStateError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '24000'


class InvalidTransactionStateError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '25000'


class ActiveSQLTransactionError(InvalidTransactionStateError):
    sqlstate: typing.ClassVar[str] = '25001'


class BranchTransactionAlreadyActiveError(InvalidTransactionStateError):
    sqlstate: typing.ClassVar[str] = '25002'


class HeldCursorRequiresSameIsolationLevelError(InvalidTransactionStateError):
    sqlstate: typing.ClassVar[str] = '25008'


class InappropriateAccessModeForBranchTransactionError(
        InvalidTransactionStateError):
    sqlstate: typing.ClassVar[str] = '25003'


class InappropriateIsolationLevelForBranchTransactionError(
        InvalidTransactionStateError):
    sqlstate: typing.ClassVar[str] = '25004'


class NoActiveSQLTransactionForBranchTransactionError(
        InvalidTransactionStateError):
    sqlstate: typing.ClassVar[str] = '25005'


class ReadOnlySQLTransactionError(InvalidTransactionStateError):
    sqlstate: typing.ClassVar[str] = '25006'


class SchemaAndDataStatementMixingNotSupportedError(
        InvalidTransactionStateError):
    sqlstate: typing.ClassVar[str] = '25007'


class NoActiveSQLTransactionError(InvalidTransactionStateError):
    sqlstate: typing.ClassVar[str] = '25P01'


class InFailedSQLTransactionError(InvalidTransactionStateError):
    sqlstate: typing.ClassVar[str] = '25P02'


class IdleInTransactionSessionTimeoutError(InvalidTransactionStateError):
    sqlstate: typing.ClassVar[str] = '25P03'


class InvalidSQLStatementNameError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '26000'


class TriggeredDataChangeViolationError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '27000'


class InvalidAuthorizationSpecificationError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '28000'


class InvalidPasswordError(InvalidAuthorizationSpecificationError):
    sqlstate: typing.ClassVar[str] = '28P01'


class DependentPrivilegeDescriptorsStillExistError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '2B000'


class DependentObjectsStillExistError(
        DependentPrivilegeDescriptorsStillExistError):
    sqlstate: typing.ClassVar[str] = '2BP01'


class InvalidTransactionTerminationError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '2D000'


class SQLRoutineError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '2F000'


class FunctionExecutedNoReturnStatementError(SQLRoutineError):
    sqlstate: typing.ClassVar[str] = '2F005'


class ModifyingSQLDataNotPermittedError(SQLRoutineError):
    sqlstate: typing.ClassVar[str] = '2F002'


class ProhibitedSQLStatementAttemptedError(SQLRoutineError):
    sqlstate: typing.ClassVar[str] = '2F003'


class ReadingSQLDataNotPermittedError(SQLRoutineError):
    sqlstate: typing.ClassVar[str] = '2F004'


class InvalidCursorNameError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '34000'


class ExternalRoutineError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '38000'


class ContainingSQLNotPermittedError(ExternalRoutineError):
    sqlstate: typing.ClassVar[str] = '38001'


class ModifyingExternalRoutineSQLDataNotPermittedError(ExternalRoutineError):
    sqlstate: typing.ClassVar[str] = '38002'


class ProhibitedExternalRoutineSQLStatementAttemptedError(
        ExternalRoutineError):
    sqlstate: typing.ClassVar[str] = '38003'


class ReadingExternalRoutineSQLDataNotPermittedError(ExternalRoutineError):
    sqlstate: typing.ClassVar[str] = '38004'


class ExternalRoutineInvocationError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '39000'


class InvalidSqlstateReturnedError(ExternalRoutineInvocationError):
    sqlstate: typing.ClassVar[str] = '39001'


class NullValueInExternalRoutineNotAllowedError(
        ExternalRoutineInvocationError):
    sqlstate: typing.ClassVar[str] = '39004'


class TriggerProtocolViolatedError(ExternalRoutineInvocationError):
    sqlstate: typing.ClassVar[str] = '39P01'


class SrfProtocolViolatedError(ExternalRoutineInvocationError):
    sqlstate: typing.ClassVar[str] = '39P02'


class EventTriggerProtocolViolatedError(ExternalRoutineInvocationError):
    sqlstate: typing.ClassVar[str] = '39P03'


class SavepointError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '3B000'


class InvalidSavepointSpecificationError(SavepointError):
    sqlstate: typing.ClassVar[str] = '3B001'


class InvalidCatalogNameError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '3D000'


class InvalidSchemaNameError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '3F000'


class TransactionRollbackError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '40000'


class TransactionIntegrityConstraintViolationError(TransactionRollbackError):
    sqlstate: typing.ClassVar[str] = '40002'


class SerializationError(TransactionRollbackError):
    sqlstate: typing.ClassVar[str] = '40001'


class StatementCompletionUnknownError(TransactionRollbackError):
    sqlstate: typing.ClassVar[str] = '40003'


class DeadlockDetectedError(TransactionRollbackError):
    sqlstate: typing.ClassVar[str] = '40P01'


class SyntaxOrAccessError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '42000'


class PostgresSyntaxError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42601'


class InsufficientPrivilegeError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42501'


class CannotCoerceError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42846'


class GroupingError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42803'


class WindowingError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P20'


class InvalidRecursionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P19'


class InvalidForeignKeyError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42830'


class InvalidNameError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42602'


class NameTooLongError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42622'


class ReservedNameError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42939'


class DatatypeMismatchError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42804'


class IndeterminateDatatypeError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P18'


class CollationMismatchError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P21'


class IndeterminateCollationError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P22'


class WrongObjectTypeError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42809'


class GeneratedAlwaysError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '428C9'


class UndefinedColumnError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42703'


class UndefinedFunctionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42883'


class UndefinedTableError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P01'


class UndefinedParameterError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P02'


class UndefinedObjectError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42704'


class DuplicateColumnError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42701'


class DuplicateCursorError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P03'


class DuplicateDatabaseError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P04'


class DuplicateFunctionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42723'


class DuplicatePreparedStatementError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P05'


class DuplicateSchemaError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P06'


class DuplicateTableError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P07'


class DuplicateAliasError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42712'


class DuplicateObjectError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42710'


class AmbiguousColumnError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42702'


class AmbiguousFunctionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42725'


class AmbiguousParameterError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P08'


class AmbiguousAliasError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P09'


class InvalidColumnReferenceError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P10'


class InvalidColumnDefinitionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42611'


class InvalidCursorDefinitionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P11'


class InvalidDatabaseDefinitionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P12'


class InvalidFunctionDefinitionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P13'


class InvalidPreparedStatementDefinitionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P14'


class InvalidSchemaDefinitionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P15'


class InvalidTableDefinitionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P16'


class InvalidObjectDefinitionError(SyntaxOrAccessError):
    sqlstate: typing.ClassVar[str] = '42P17'


class WithCheckOptionViolationError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '44000'


class InsufficientResourcesError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '53000'


class DiskFullError(InsufficientResourcesError):
    sqlstate: typing.ClassVar[str] = '53100'


class OutOfMemoryError(InsufficientResourcesError):
    sqlstate: typing.ClassVar[str] = '53200'


class TooManyConnectionsError(InsufficientResourcesError):
    sqlstate: typing.ClassVar[str] = '53300'


class ConfigurationLimitExceededError(InsufficientResourcesError):
    sqlstate: typing.ClassVar[str] = '53400'


class ProgramLimitExceededError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '54000'


class StatementTooComplexError(ProgramLimitExceededError):
    sqlstate: typing.ClassVar[str] = '54001'


class TooManyColumnsError(ProgramLimitExceededError):
    sqlstate: typing.ClassVar[str] = '54011'


class TooManyArgumentsError(ProgramLimitExceededError):
    sqlstate: typing.ClassVar[str] = '54023'


class ObjectNotInPrerequisiteStateError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '55000'


class ObjectInUseError(ObjectNotInPrerequisiteStateError):
    sqlstate: typing.ClassVar[str] = '55006'


class CantChangeRuntimeParamError(ObjectNotInPrerequisiteStateError):
    sqlstate: typing.ClassVar[str] = '55P02'


class LockNotAvailableError(ObjectNotInPrerequisiteStateError):
    sqlstate: typing.ClassVar[str] = '55P03'


class UnsafeNewEnumValueUsageError(ObjectNotInPrerequisiteStateError):
    sqlstate: typing.ClassVar[str] = '55P04'


class OperatorInterventionError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '57000'


class QueryCanceledError(OperatorInterventionError):
    sqlstate: typing.ClassVar[str] = '57014'


class AdminShutdownError(OperatorInterventionError):
    sqlstate: typing.ClassVar[str] = '57P01'


class CrashShutdownError(OperatorInterventionError):
    sqlstate: typing.ClassVar[str] = '57P02'


class CannotConnectNowError(OperatorInterventionError):
    sqlstate: typing.ClassVar[str] = '57P03'


class DatabaseDroppedError(OperatorInterventionError):
    sqlstate: typing.ClassVar[str] = '57P04'


class IdleSessionTimeoutError(OperatorInterventionError):
    sqlstate: typing.ClassVar[str] = '57P05'


class PostgresSystemError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '58000'


class PostgresIOError(PostgresSystemError):
    sqlstate: typing.ClassVar[str] = '58030'


class UndefinedFileError(PostgresSystemError):
    sqlstate: typing.ClassVar[str] = '58P01'


class DuplicateFileError(PostgresSystemError):
    sqlstate: typing.ClassVar[str] = '58P02'


class SnapshotTooOldError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = '72000'


class ConfigFileError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = 'F0000'


class LockFileExistsError(ConfigFileError):
    sqlstate: typing.ClassVar[str] = 'F0001'


class FDWError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = 'HV000'


class FDWColumnNameNotFoundError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV005'


class FDWDynamicParameterValueNeededError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV002'


class FDWFunctionSequenceError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV010'


class FDWInconsistentDescriptorInformationError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV021'


class FDWInvalidAttributeValueError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV024'


class FDWInvalidColumnNameError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV007'


class FDWInvalidColumnNumberError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV008'


class FDWInvalidDataTypeError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV004'


class FDWInvalidDataTypeDescriptorsError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV006'


class FDWInvalidDescriptorFieldIdentifierError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV091'


class FDWInvalidHandleError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00B'


class FDWInvalidOptionIndexError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00C'


class FDWInvalidOptionNameError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00D'


class FDWInvalidStringLengthOrBufferLengthError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV090'


class FDWInvalidStringFormatError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00A'


class FDWInvalidUseOfNullPointerError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV009'


class FDWTooManyHandlesError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV014'


class FDWOutOfMemoryError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV001'


class FDWNoSchemasError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00P'


class FDWOptionNameNotFoundError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00J'


class FDWReplyHandleError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00K'


class FDWSchemaNotFoundError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00Q'


class FDWTableNotFoundError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00R'


class FDWUnableToCreateExecutionError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00L'


class FDWUnableToCreateReplyError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00M'


class FDWUnableToEstablishConnectionError(FDWError):
    sqlstate: typing.ClassVar[str] = 'HV00N'


class PLPGSQLError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = 'P0000'


class RaiseError(PLPGSQLError):
    sqlstate: typing.ClassVar[str] = 'P0001'


class NoDataFoundError(PLPGSQLError):
    sqlstate: typing.ClassVar[str] = 'P0002'


class TooManyRowsError(PLPGSQLError):
    sqlstate: typing.ClassVar[str] = 'P0003'


class AssertError(PLPGSQLError):
    sqlstate: typing.ClassVar[str] = 'P0004'


class InternalServerError(_base.PostgresError):
    sqlstate: typing.ClassVar[str] = 'XX000'


class DataCorruptedError(InternalServerError):
    sqlstate: typing.ClassVar[str] = 'XX001'


class IndexCorruptedError(InternalServerError):
    sqlstate: typing.ClassVar[str] = 'XX002'


__all__ = [
    'ActiveSQLTransactionError', 'AdminShutdownError',
    'AmbiguousAliasError', 'AmbiguousColumnError',
    'AmbiguousFunctionError', 'AmbiguousParameterError',
    'ArraySubscriptError', 'AssertError', 'BadCopyFileFormatError',
    'BranchTransactionAlreadyActiveError', 'CannotCoerceError',
    'CannotConnectNowError', 'CantChangeRuntimeParamError',
    'CardinalityViolationError', 'CaseNotFoundError',
    'CharacterNotInRepertoireError', 'CheckViolationError',
    'ClientCannotConnectError', 'CollationMismatchError',
    'ConfigFileError', 'ConfigurationLimitExceededError',
    'ConnectionDoesNotExistError', 'ConnectionFailureError',
    'ConnectionRejectionError', 'ContainingSQLNotPermittedError',
    'CrashShutdownError', 'DataCorruptedError', 'DataError',
    'DatabaseDroppedError', 'DatatypeMismatchError',
    'DatetimeFieldOverflowError', 'DeadlockDetectedError',
    'DependentObjectsStillExistError',
    'DependentPrivilegeDescriptorsStillExistError', 'DeprecatedFeature',
    'DiagnosticsError', 'DiskFullError', 'DivisionByZeroError',
    'DuplicateAliasError', 'DuplicateColumnError', 'DuplicateCursorError',
    'DuplicateDatabaseError', 'DuplicateFileError',
    'DuplicateFunctionError', 'DuplicateJsonObjectKeyValueError',
    'DuplicateObjectError', 'DuplicatePreparedStatementError',
    'DuplicateSchemaError', 'DuplicateTableError',
    'DynamicResultSetsReturned', 'ErrorInAssignmentError',
    'EscapeCharacterConflictError', 'EventTriggerProtocolViolatedError',
    'ExclusionViolationError', 'ExternalRoutineError',
    'ExternalRoutineInvocationError', 'FDWColumnNameNotFoundError',
    'FDWDynamicParameterValueNeededError', 'FDWError',
    'FDWFunctionSequenceError',
    'FDWInconsistentDescriptorInformationError',
    'FDWInvalidAttributeValueError', 'FDWInvalidColumnNameError',
    'FDWInvalidColumnNumberError', 'FDWInvalidDataTypeDescriptorsError',
    'FDWInvalidDataTypeError', 'FDWInvalidDescriptorFieldIdentifierError',
    'FDWInvalidHandleError', 'FDWInvalidOptionIndexError',
    'FDWInvalidOptionNameError', 'FDWInvalidStringFormatError',
    'FDWInvalidStringLengthOrBufferLengthError',
    'FDWInvalidUseOfNullPointerError', 'FDWNoSchemasError',
    'FDWOptionNameNotFoundError', 'FDWOutOfMemoryError',
    'FDWReplyHandleError', 'FDWSchemaNotFoundError',
    'FDWTableNotFoundError', 'FDWTooManyHandlesError',
    'FDWUnableToCreateExecutionError', 'FDWUnableToCreateReplyError',
    'FDWUnableToEstablishConnectionError', 'FeatureNotSupportedError',
    'ForeignKeyViolationError', 'FunctionExecutedNoReturnStatementError',
    'GeneratedAlwaysError', 'GroupingError',
    'HeldCursorRequiresSameIsolationLevelError',
    'IdleInTransactionSessionTimeoutError', 'IdleSessionTimeoutError',
    'ImplicitZeroBitPadding', 'InFailedSQLTransactionError',
    'InappropriateAccessModeForBranchTransactionError',
    'InappropriateIsolationLevelForBranchTransactionError',
    'IndeterminateCollationError', 'IndeterminateDatatypeError',
    'IndexCorruptedError', 'IndicatorOverflowError',
    'InsufficientPrivilegeError', 'InsufficientResourcesError',
    'IntegrityConstraintViolationError', 'InternalServerError',
    'IntervalFieldOverflowError', 'InvalidArgumentForLogarithmError',
    'InvalidArgumentForNthValueFunctionError',
    'InvalidArgumentForNtileFunctionError',
    'InvalidArgumentForPowerFunctionError',
    'InvalidArgumentForSQLJsonDatetimeFunctionError',
    'InvalidArgumentForWidthBucketFunctionError',
    'InvalidAuthorizationSpecificationError',
    'InvalidBinaryRepresentationError', 'InvalidCachedStatementError',
    'InvalidCatalogNameError', 'InvalidCharacterValueForCastError',
    'InvalidColumnDefinitionError', 'InvalidColumnReferenceError',
    'InvalidCursorDefinitionError', 'InvalidCursorNameError',
    'InvalidCursorStateError', 'InvalidDatabaseDefinitionError',
    'InvalidDatetimeFormatError', 'InvalidEscapeCharacterError',
    'InvalidEscapeOctetError', 'InvalidEscapeSequenceError',
    'InvalidForeignKeyError', 'InvalidFunctionDefinitionError',
    'InvalidGrantOperationError', 'InvalidGrantorError',
    'InvalidIndicatorParameterValueError', 'InvalidJsonTextError',
    'InvalidLocatorSpecificationError', 'InvalidNameError',
    'InvalidObjectDefinitionError', 'InvalidParameterValueError',
    'InvalidPasswordError', 'InvalidPrecedingOrFollowingSizeError',
    'InvalidPreparedStatementDefinitionError', 'InvalidRecursionError',
    'InvalidRegularExpressionError', 'InvalidRoleSpecificationError',
    'InvalidRowCountInLimitClauseError',
    'InvalidRowCountInResultOffsetClauseError',
    'InvalidSQLJsonSubscriptError', 'InvalidSQLStatementNameError',
    'InvalidSavepointSpecificationError', 'InvalidSchemaDefinitionError',
    'InvalidSchemaNameError', 'InvalidSqlstateReturnedError',
    'InvalidTableDefinitionError', 'InvalidTablesampleArgumentError',
    'InvalidTablesampleRepeatError', 'InvalidTextRepresentationError',
    'InvalidTimeZoneDisplacementValueError',
    'InvalidTransactionInitiationError', 'InvalidTransactionStateError',
    'InvalidTransactionTerminationError',
    'InvalidUseOfEscapeCharacterError', 'InvalidXmlCommentError',
    'InvalidXmlContentError', 'InvalidXmlDocumentError',
    'InvalidXmlProcessingInstructionError', 'LocatorError',
    'LockFileExistsError', 'LockNotAvailableError',
    'ModifyingExternalRoutineSQLDataNotPermittedError',
    'ModifyingSQLDataNotPermittedError', 'MoreThanOneSQLJsonItemError',
    'MostSpecificTypeMismatchError', 'NameTooLongError',
    'NoActiveSQLTransactionError',
    'NoActiveSQLTransactionForBranchTransactionError',
    'NoAdditionalDynamicResultSetsReturned', 'NoData', 'NoDataFoundError',
    'NoSQLJsonItemError', 'NonNumericSQLJsonItemError',
    'NonUniqueKeysInAJsonObjectError',
    'NonstandardUseOfEscapeCharacterError', 'NotAnXmlDocumentError',
    'NotNullViolationError', 'NullValueEliminatedInSetFunction',
    'NullValueInExternalRoutineNotAllowedError',
    'NullValueNoIndicatorParameterError', 'NullValueNotAllowedError',
    'NumericValueOutOfRangeError', 'ObjectInUseError',
    'ObjectNotInPrerequisiteStateError', 'OperatorInterventionError',
    'OutOfMemoryError', 'PLPGSQLError', 'PostgresConnectionError',
    'PostgresFloatingPointError', 'PostgresIOError',
    'PostgresSyntaxError', 'PostgresSystemError', 'PostgresWarning',
    'PrivilegeNotGranted', 'PrivilegeNotRevoked',
    'ProgramLimitExceededError',
    'ProhibitedExternalRoutineSQLStatementAttemptedError',
    'ProhibitedSQLStatementAttemptedError', 'ProtocolViolationError',
    'QueryCanceledError', 'RaiseError', 'ReadOnlySQLTransactionError',
    'ReadingExternalRoutineSQLDataNotPermittedError',
    'ReadingSQLDataNotPermittedError', 'ReservedNameError',
    'RestrictViolationError', 'SQLJsonArrayNotFoundError',
    'SQLJsonItemCannotBeCastToTargetTypeError',
    'SQLJsonMemberNotFoundError', 'SQLJsonNumberNotFoundError',
    'SQLJsonObjectNotFoundError', 'SQLJsonScalarRequiredError',
    'SQLRoutineError', 'SQLStatementNotYetCompleteError',
    'SavepointError', 'SchemaAndDataStatementMixingNotSupportedError',
    'SequenceGeneratorLimitExceededError', 'SerializationError',
    'SingletonSQLJsonItemRequiredError', 'SnapshotTooOldError',
    'SrfProtocolViolatedError',
    'StackedDiagnosticsAccessedWithoutActiveHandlerError',
    'StatementCompletionUnknownError', 'StatementTooComplexError',
    'StringDataLengthMismatchError', 'StringDataRightTruncation',
    'StringDataRightTruncationError', 'SubstringError',
    'SyntaxOrAccessError', 'TooManyArgumentsError', 'TooManyColumnsError',
    'TooManyConnectionsError', 'TooManyJsonArrayElementsError',
    'TooManyJsonObjectMembersError', 'TooManyRowsError',
    'TransactionIntegrityConstraintViolationError',
    'TransactionResolutionUnknownError', 'TransactionRollbackError',
    'TriggerProtocolViolatedError', 'TriggeredActionError',
    'TriggeredDataChangeViolationError', 'TrimError',
    'UndefinedColumnError', 'UndefinedFileError',
    'UndefinedFunctionError', 'UndefinedObjectError',
    'UndefinedParameterError', 'UndefinedTableError',
    'UniqueViolationError', 'UnsafeNewEnumValueUsageError',
    'UnterminatedCStringError', 'UntranslatableCharacterError',
    'WindowingError', 'WithCheckOptionViolationError',
    'WrongObjectTypeError', 'ZeroLengthCharacterStringError'
]

__all__ += _base.__all__
