import pickle
from typing import Union

import numpy as np


def set_query_dtype(query: str, dtype: Union[str, np.dtype]) -> str:
    """
    Augment the query string with the result numpy data type.

    """
    dtype = np.dtype(dtype)
    if not dtype.fields:
        raise ValueError("The data type must be a structure")
    # we cannot write a pointer because the object is not referenced
    # directly from the string and may die
    serialized_dtype = pickle.dumps(dtype).hex()
    return f"🚀{serialized_dtype}🚀\n{query}"
