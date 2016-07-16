# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import sys


__all__ = ('PostgresError', 'FatalPostgresError', 'UnknownPostgresError',
           'InterfaceError')


class PostgresMessageMeta(type):
    _message_map = {}
    _field_map = {
        'S': 'severity',
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

        if (cls.__module__ == 'asyncpg' or
                cls.__module__.startswith('asyncpg.')):
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
    def __str__(self):
        msg = self.message
        if self.detail:
            msg += '\nDETAIL:  {}'.format(self.detail)
        if self.hint:
            msg += '\nHINT:  {}'.format(self.hint)

        return msg

    @classmethod
    def new(cls, fields, query=None):
        errcode = fields.get('C')
        mcls = cls.__class__
        exccls = mcls.get_message_class_for_sqlstate(errcode)
        mapped = {
            'query': query
        }

        for k, v in fields.items():
            field = mcls._field_map.get(k)
            if field:
                mapped[field] = v

        e = exccls(mapped.get('message'))
        e.__dict__.update(mapped)

        return e


class PostgresError(Exception, PostgresMessage):
    """Base class for all Postgres errors."""


class FatalPostgresError(PostgresError):
    """A fatal error that should result in server disconnection."""


class UnknownPostgresError(FatalPostgresError):
    """An error with an unknown SQLSTATE code."""


class InterfaceError(Exception):
    """An error caused by improper use of asyncpg API."""
