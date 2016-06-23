__all__ = ()  # Will be completed by ErrorMeta


class ErrorMeta(type):
    _error_map = {}

    def __new__(mcls, name, bases, dct):
        global __all__
        __all__ += (name,)

        cls = super().__new__(mcls, name, bases, dct)
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


class OperatorInterventionError(FatalError):
    code = '57000'


class ServerNotReadyError(OperatorInterventionError):
    code = '57P03'
