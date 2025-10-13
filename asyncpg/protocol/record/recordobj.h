#ifndef APG_RECORDOBJ_H
#define APG_RECORDOBJ_H

#include <Python.h>


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


#define ApgRecord_SET_ITEM(op, i, v) \
			(((ApgRecordObject *)(op))->ob_item[i] = v)

#define ApgRecord_GET_ITEM(op, i) \
			(((ApgRecordObject *)(op))->ob_item[i])

#endif
