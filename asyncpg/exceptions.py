__all__ = ()  # Will be completed by ErrorMeta


class ErrorMeta(type):
    _error_map = {}

    def __new__(mcls, name, bases, dct):
        global __all__

        cls = super().__new__(mcls, name, bases, dct)
        if cls.__module__ == 'asyncpg.exceptions':
            __all__ += (name,)

        code = dct.get('code')
        if code is not None:
            mcls._error_map[code] = cls

        return cls

    @classmethod
    def get_error_for_code(mcls, code):
        return mcls._error_map.get(code, Error)


class Error(Exception, metaclass=ErrorMeta):
    pass


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
