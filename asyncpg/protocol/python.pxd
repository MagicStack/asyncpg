# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from cpython cimport Py_buffer

cdef extern from "Python.h":
    void* PyMem_Malloc(size_t n)
    void* PyMem_Realloc(void *p, size_t n)
    void* PyMem_Calloc(size_t nelem, size_t elsize)  # Python >= 3.5!
    void PyMem_Free(void *p)

    int PyByteArray_Check(object)
    ssize_t PyByteArray_Size(object)
    int PyByteArray_Resize(object, ssize_t)
    object PyByteArray_FromStringAndSize(const char *, ssize_t)

    int PyMemoryView_Check(object)
    Py_buffer *PyMemoryView_GET_BUFFER(object)
    object PyMemoryView_FromMemory(char *mem, ssize_t size, int flags)
    object PyMemoryView_GetContiguous(object, int buffertype, char order)

    object PyUnicode_FromString(const char *u)
    char* PyUnicode_AsUTF8AndSize(object unicode, ssize_t *size) except NULL
    char* PyByteArray_AsString(object)
    Py_UCS4* PyUnicode_AsUCS4Copy(object)
    object PyUnicode_FromKindAndData(
        int kind, const void *buffer, Py_ssize_t size)

    int PyUnicode_4BYTE_KIND
