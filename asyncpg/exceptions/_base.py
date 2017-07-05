# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncpg
import sys


__all__ = ('PostgresError', 'FatalPostgresError', 'UnknownPostgresError',
           'InterfaceError', 'PostgresLogMessage')


def _is_asyncpg_class(cls):
    modname = cls.__module__
    return modname == 'asyncpg' or modname.startswith('asyncpg.')


class PostgresMessageMeta(type):

    _message_map = {}
    _field_map = {
        'S': 'severity',
        'V': 'severity_en',
        'C': 'sqlstate',
        'M': 'message',
        'D': 'detail',
        'H': 'hint',
        'P': 'position',
        'p': 'internal_position',
        'q': 'internal_query',
        'W': 'context',
        's': 'schema_name',
        't': 'table_name',
        'c': 'column_name',
        'd': 'data_type_name',
        'n': 'constraint_name',
        'F': 'server_source_filename',
        'L': 'server_source_line',
        'R': 'server_source_function'
    }

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)
        if cls.__module__ == mcls.__module__ and name == 'PostgresMessage':
            for f in mcls._field_map.values():
                setattr(cls, f, None)

        if _is_asyncpg_class(cls):
            mod = sys.modules[cls.__module__]
            if hasattr(mod, name):
                raise RuntimeError('exception class redefinition: {}'.format(
                    name))

        code = dct.get('sqlstate')
        if code is not None:
            existing = mcls._message_map.get(code)
            if existing is not None:
                raise TypeError('{} has duplicate SQLSTATE code, which is'
                                'already defined by {}'.format(
                                    name, existing.__name__))
            mcls._message_map[code] = cls

        return cls

    @classmethod
    def get_message_class_for_sqlstate(mcls, code):
        return mcls._message_map.get(code, UnknownPostgresError)


class PostgresMessage(metaclass=PostgresMessageMeta):

    @classmethod
    def _get_error_class(cls, fields):
        sqlstate = fields.get('C')
        return type(cls).get_message_class_for_sqlstate(sqlstate)

    @classmethod
    def _get_error_dict(cls, fields, query):
        dct = {
            'query': query
        }

        field_map = type(cls)._field_map
        for k, v in fields.items():
            field = field_map.get(k)
            if field:
                dct[field] = v

        return dct

    @classmethod
    def _make_constructor(cls, fields, query=None):
        dct = cls._get_error_dict(fields, query)

        exccls = cls._get_error_class(fields)
        message = dct.get('message', '')

        # PostgreSQL will raise an exception when it detects
        # that the result type of the query has changed from
        # when the statement was prepared.
        #
        # The original error is somewhat cryptic and unspecific,
        # so we raise a custom subclass that is easier to handle
        # and identify.
        #
        # Note that we specifically do not rely on the error
        # message, as it is localizable.
        is_icse = (
            exccls.__name__ == 'FeatureNotSupportedError' and
            _is_asyncpg_class(exccls) and
            dct.get('server_source_function') == 'RevalidateCachedQuery'
        )

        if is_icse:
            exceptions = sys.modules[exccls.__module__]
            exccls = exceptions.InvalidCachedStatementError
            message = ('cached statement plan is invalid due to a database '
                       'schema or configuration change')

        return exccls, message, dct

    def as_dict(self):
        dct = {}
        for f in type(self)._field_map.values():
            val = getattr(self, f)
            if val is not None:
                dct[f] = val
        return dct


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


class InterfaceError(Exception):
    """An error caused by improper use of asyncpg API."""


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
                exccls = asyncpg.PostgresWarning

        if issubclass(exccls, (BaseException, Warning)):
            msg = exccls(message_text)
        else:
            msg = exccls()

        msg.__dict__.update(dct)
        return msg
