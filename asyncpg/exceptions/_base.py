# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

# PostgresMessageMeta imported for backward compatibility
from ._postgres_message import (  # noqa: F401
    PostgresMessage as PostgresMessage,
    PostgresMessageMeta as PostgresMessageMeta,
)


__all__ = ('PostgresError', 'FatalPostgresError', 'UnknownPostgresError',
           'InterfaceError', 'InterfaceWarning', 'PostgresLogMessage',
           'ClientConfigurationError',
           'InternalClientError', 'OutdatedSchemaCacheError', 'ProtocolError',
           'UnsupportedClientFeatureError', 'TargetServerAttributeNotMatched',
           'UnsupportedServerFeatureError')


class PostgresError(PostgresMessage, Exception):
    """Base class for all Postgres errors."""

    def __str__(self):
        msg = self.args[0]
        if self.detail:
            msg += '\nDETAIL:  {}'.format(self.detail)
        if self.hint:
            msg += '\nHINT:  {}'.format(self.hint)

        return msg

    @classmethod
    def new(cls, fields, query=None):
        exccls, message, dct = cls._make_constructor(fields, query)
        ex = exccls(message)
        ex.__dict__.update(dct)
        return ex


class FatalPostgresError(PostgresError):
    """A fatal error that should result in server disconnection."""


class UnknownPostgresError(FatalPostgresError):
    """An error with an unknown SQLSTATE code."""


class InterfaceMessage:
    def __init__(self, *, detail=None, hint=None):
        self.detail = detail
        self.hint = hint

    def __str__(self):
        msg = self.args[0]
        if self.detail:
            msg += '\nDETAIL:  {}'.format(self.detail)
        if self.hint:
            msg += '\nHINT:  {}'.format(self.hint)

        return msg


class InterfaceError(InterfaceMessage, Exception):
    """An error caused by improper use of asyncpg API."""

    def __init__(self, msg, *, detail=None, hint=None):
        InterfaceMessage.__init__(self, detail=detail, hint=hint)
        Exception.__init__(self, msg)

    def with_msg(self, msg):
        return type(self)(
            msg,
            detail=self.detail,
            hint=self.hint,
        ).with_traceback(
            self.__traceback__
        )


class ClientConfigurationError(InterfaceError, ValueError):
    """An error caused by improper client configuration."""


class DataError(InterfaceError, ValueError):
    """An error caused by invalid query input."""


class UnsupportedClientFeatureError(InterfaceError):
    """Requested feature is unsupported by asyncpg."""


class UnsupportedServerFeatureError(InterfaceError):
    """Requested feature is unsupported by PostgreSQL server."""


class InterfaceWarning(InterfaceMessage, UserWarning):
    """A warning caused by an improper use of asyncpg API."""

    def __init__(self, msg, *, detail=None, hint=None):
        InterfaceMessage.__init__(self, detail=detail, hint=hint)
        UserWarning.__init__(self, msg)


class InternalClientError(Exception):
    """All unexpected errors not classified otherwise."""


class ProtocolError(InternalClientError):
    """Unexpected condition in the handling of PostgreSQL protocol input."""


class TargetServerAttributeNotMatched(InternalClientError):
    """Could not find a host that satisfies the target attribute requirement"""


class OutdatedSchemaCacheError(InternalClientError):
    """A value decoding error caused by a schema change before row fetching."""

    def __init__(self, msg, *, schema=None, data_type=None, position=None):
        super().__init__(msg)
        self.schema_name = schema
        self.data_type_name = data_type
        self.position = position


class PostgresLogMessage(PostgresMessage):
    """A base class for non-error server messages."""

    def __str__(self):
        return '{}: {}'.format(type(self).__name__, self.message)

    def __setattr__(self, name, val):
        raise TypeError('instances of {} are immutable'.format(
            type(self).__name__))

    @classmethod
    def new(cls, fields, query=None):
        exccls, message_text, dct = cls._make_constructor(fields, query)

        if exccls is UnknownPostgresError:
            exccls = PostgresLogMessage

        if exccls is PostgresLogMessage:
            severity = dct.get('severity_en') or dct.get('severity')
            if severity and severity.upper() == 'WARNING':
                from . import PostgresWarning
                exccls = PostgresWarning

        if issubclass(exccls, (BaseException, Warning)):
            msg = exccls(message_text)
        else:
            msg = exccls()

        msg.__dict__.update(dct)
        return msg
