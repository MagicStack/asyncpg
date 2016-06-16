from cpython cimport Py_buffer

cdef extern from "Python.h":
    void* PyMem_Malloc(size_t n)
    void* PyMem_Realloc(void *p, size_t n)
    void* PyMem_Calloc(size_t nelem, size_t elsize)  # Python >= 3.5!
    void PyMem_Free(void *p)

    int PyMemoryView_Check(object)
    Py_buffer *PyMemoryView_GET_BUFFER(object)
