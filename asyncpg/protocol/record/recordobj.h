#ifndef APG_RECORDOBJ_H
#define APG_RECORDOBJ_H

#include "Python.h"


/* Largest record to save on free list */
#define ApgRecord_MAXSAVESIZE 20

/* Maximum number of records of each size to save */
#define ApgRecord_MAXFREELIST 2000


typedef struct {
    PyObject_HEAD
    PyObject *mapping;
    PyObject *keys;
} ApgRecordDescObject;


typedef struct {
    PyObject_VAR_HEAD
    Py_hash_t self_hash;
    ApgRecordDescObject *desc;
    PyObject *ob_item[1];

    /* ob_item contains space for 'ob_size' elements.
     * Items must normally not be NULL, except during construction when
     * the record is not yet visible outside the function that builds it.
     */
} ApgRecordObject;


extern PyTypeObject ApgRecord_Type;
extern PyTypeObject ApgRecordIter_Type;
extern PyTypeObject ApgRecordItems_Type;

extern PyTypeObject ApgRecordDesc_Type;

#define ApgRecord_Check(self) PyObject_TypeCheck(self, &ApgRecord_Type)
#define ApgRecord_CheckExact(o) (Py_TYPE(o) == &ApgRecord_Type)
#define ApgRecordDesc_CheckExact(o) (Py_TYPE(o) == &ApgRecordDesc_Type)

#define ApgRecord_SET_ITEM(op, i, v) \
			(((ApgRecordObject *)(op))->ob_item[i] = v)
#define ApgRecord_GET_ITEM(op, i) \
			(((ApgRecordObject *)(op))->ob_item[i])

PyTypeObject *ApgRecord_InitTypes(void);
PyObject *ApgRecord_New(PyTypeObject *, PyObject *, Py_ssize_t);
PyObject *ApgRecordDesc_New(PyObject *, PyObject *);

#endif
