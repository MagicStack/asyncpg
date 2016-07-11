#ifndef APG_RECORDOBJ_H
#define APG_RECORDOBJ_H

#include "Python.h"


/* Largest record to save on free list */
#define ApgRecord_MAXSAVESIZE 20

/* Maximum number of records of each size to save */
#define ApgRecord_MAXFREELIST 2000


typedef struct {
    PyObject_VAR_HEAD
    PyObject *mapping;
    PyObject *ob_item[1];

    /* ob_item contains space for 'ob_size' elements.
     * Items must normally not be NULL, except during construction when
     * the record is not yet visible outside the function that builds it.
     */
} ApgRecordObject;

extern PyTypeObject ApgRecord_Type;

#define ApgRecord_CheckExact(o) (Py_TYPE(o) == &ApgRecord_Type)
#define ApgRecord_SET_ITEM(op, i, v) \
			(((ApgRecordObject *)(op))->ob_item[i] = v)

PyObject * ApgRecord_New(PyObject *, Py_ssize_t);

#endif
