import ctypes
from typing import Union

import numpy as np


class DtypedStr(str):
    """A string that keeps a reference to the query result dtype."""

    def __new__(cls, value: str, dtype: np.dtype) -> "DtypedStr":
        """Override str.__new__ to set "dtype" attribute."""
        obj = super().__new__(cls, value)
        obj.dtype = dtype
        return obj


_ptr_size = ctypes.sizeof(ctypes.py_object)


def set_query_dtype(query: str, dtype: Union[str, np.dtype]) -> DtypedStr:
    """
    Augment the query string with the result numpy data type.

    """
    dtype = np.dtype(dtype)
    if not dtype.fields:
        raise ValueError("The data type must be a structure")
    ptr = id(dtype).to_bytes(_ptr_size, 'little').hex()
    return DtypedStr(f"🚀{ptr}\n{query}", dtype)
