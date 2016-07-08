##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


__all__ = ('PostgresError', 'FatalPostgresError', 'UnknownPostgresError')


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
        if cls.__module__ == mcls.__module__ and name == '_PostgresMessage':
            for f in mcls._field_map.values():
                setattr(cls, f, None)

        code = dct.get('sqlstate')
        if code is not None:
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
