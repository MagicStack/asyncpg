import pickle
from typing import Union

import numpy as np


def set_query_dtype(query: str, dtype: Union[bytes, str, np.dtype]) -> str:
    """
    Augment the query string with the result numpy data type.

    :param query: SQL query to augment.
    :param dtype: numpy dtype-compatible or pickled numpy dtype. \
                  The dtype must be structured.
    :return: Augmented SQL query with the embedded dtype.
    """
    if isinstance(dtype, bytes):
        # assume already serialized
        serialized_dtype = dtype.hex()
    else:
        dtype = np.dtype(dtype)
        if not dtype.fields:
            raise ValueError("The data type must be a structure")
        # we cannot write a pointer because the object is not referenced
        # directly from the string and may die
        serialized_dtype = pickle.dumps(dtype).hex()
    return f"--🚀{serialized_dtype}🚀\n{query}"
