__all__ = ()  # Will be completed by ErrorMeta


class ErrorMeta(type):
    _error_map = {}
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
        global __all__

        cls = super().__new__(mcls, name, bases, dct)
        if cls.__module__ == 'asyncpg.exceptions':
            __all__ += (name,)

            if name == 'Error':
                for f in mcls._field_map.values():
                    setattr(cls, f, None)

        code = dct.get('code')
        if code is not None:
            mcls._error_map[code] = cls

        return cls

    @classmethod
    def get_error_for_code(mcls, code):
        return mcls._error_map.get(code, Error)


class Error(Exception, metaclass=ErrorMeta):
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
        exccls = mcls.get_error_for_code(errcode)
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


class FatalError(Error):
    pass


class ConnectionError(FatalError):
    code = '08000'


class ClientCannotConnectError(ConnectionError):
    code = '08001'


class ConnectionRejectionError(ConnectionError):
    code = '08004'


class OperatorInterventionError(FatalError):
    code = '57000'


class AuthenticationSpecificationError(FatalError):
    code = '28000'


class ServerNotReadyError(OperatorInterventionError):
    code = '57P03'
