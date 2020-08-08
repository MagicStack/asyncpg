/* Big parts of this file are copied (with modifications) from
   CPython/Objects/tupleobject.c.

   Portions Copyright (c) PSF (and other CPython copyright holders).
   Portions Copyright (c) 2016-present MagicStack Inc.
   License: PSFL v2; see CPython/LICENSE for details.
*/

#include "recordobj.h"


static PyObject * record_iter(PyObject *);
static PyObject * record_new_items_iter(PyObject *);

static ApgRecordObject *free_list[ApgRecord_MAXSAVESIZE];
static int numfree[ApgRecord_MAXSAVESIZE];


PyObject *
ApgRecord_New(PyObject *desc, Py_ssize_t size)
{
    ApgRecordObject *o;
    Py_ssize_t i;

    if (size < 0 || desc == NULL || !ApgRecordDesc_CheckExact(desc)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (size < ApgRecord_MAXSAVESIZE && (o = free_list[size]) != NULL) {
        free_list[size] = (ApgRecordObject *) o->ob_item[0];
        numfree[size]--;
        _Py_NewReference((PyObject *)o);
    }
    else {
        /* Check for overflow */
        if ((size_t)size > ((size_t)PY_SSIZE_T_MAX - sizeof(ApgRecordObject) -
                    sizeof(PyObject *)) / sizeof(PyObject *)) {
            return PyErr_NoMemory();
        }
        o = PyObject_GC_NewVar(ApgRecordObject, &ApgRecord_Type, size);
        if (o == NULL) {
            return NULL;
        }
    }

    for (i = 0; i < size; i++) {
        o->ob_item[i] = NULL;
    }

    Py_INCREF(desc);
    o->desc = (ApgRecordDescObject*)desc;
    o->self_hash = -1;
    PyObject_GC_Track(o);
    return (PyObject *) o;
}


static void
record_dealloc(ApgRecordObject *o)
{
    Py_ssize_t i;
    Py_ssize_t len = Py_SIZE(o);

    PyObject_GC_UnTrack(o);

    o->self_hash = -1;

    Py_CLEAR(o->desc);

    Py_TRASHCAN_SAFE_BEGIN(o)
    if (len > 0) {
        i = len;
        while (--i >= 0) {
            Py_CLEAR(o->ob_item[i]);
        }

        if (len < ApgRecord_MAXSAVESIZE &&
            numfree[len] < ApgRecord_MAXFREELIST &&
            ApgRecord_CheckExact(o))
        {
            o->ob_item[0] = (PyObject *) free_list[len];
            numfree[len]++;
            free_list[len] = o;
            goto done; /* return */
        }
    }
    Py_TYPE(o)->tp_free((PyObject *)o);
done:
    Py_TRASHCAN_SAFE_END(o)
}


static int
record_traverse(ApgRecordObject *o, visitproc visit, void *arg)
{
    Py_ssize_t i;

    Py_VISIT(o->desc);

    for (i = Py_SIZE(o); --i >= 0;) {
        if (o->ob_item[i] != NULL) {
            Py_VISIT(o->ob_item[i]);
        }
    }

    return 0;
}


static Py_ssize_t
record_length(ApgRecordObject *o)
{
    return Py_SIZE(o);
}


#if PY_VERSION_HEX >= 0x03080000

#if SIZEOF_PY_UHASH_T > 4
#define _PyHASH_XXPRIME_1 ((Py_uhash_t)11400714785074694791ULL)
#define _PyHASH_XXPRIME_2 ((Py_uhash_t)14029467366897019727ULL)
#define _PyHASH_XXPRIME_5 ((Py_uhash_t)2870177450012600261ULL)
#define _PyHASH_XXROTATE(x) ((x << 31) | (x >> 33))  /* Rotate left 31 bits */
#else
#define _PyHASH_XXPRIME_1 ((Py_uhash_t)2654435761UL)
#define _PyHASH_XXPRIME_2 ((Py_uhash_t)2246822519UL)
#define _PyHASH_XXPRIME_5 ((Py_uhash_t)374761393UL)
#define _PyHASH_XXROTATE(x) ((x << 13) | (x >> 19))  /* Rotate left 13 bits */
#endif

static Py_hash_t
record_hash(ApgRecordObject *v)
{
    Py_uhash_t acc = _PyHASH_XXPRIME_5;
    size_t i, len = (size_t)Py_SIZE(v);
    PyObject **els = v->ob_item;
    for (i = 0; i < len; i++) {
        Py_uhash_t lane = (Py_uhash_t)PyObject_Hash(els[i]);
        if (lane == (Py_uhash_t)-1) {
            return -1;
        }
        acc += lane * _PyHASH_XXPRIME_2;
        acc = _PyHASH_XXROTATE(acc);
        acc *= _PyHASH_XXPRIME_1;
    }

    /* Add input length, mangled to keep the historical value of hash(()). */
    acc += len ^ (_PyHASH_XXPRIME_5 ^ 3527539UL);

    if (acc == (Py_uhash_t)-1) {
        return 1546275796;
    }
    return (Py_hash_t)acc;
}

#else

static Py_hash_t
record_hash(ApgRecordObject *v)
{
    Py_uhash_t x;  /* Unsigned for defined overflow behavior. */
    Py_hash_t y;
    Py_ssize_t len;
    PyObject **p;
    Py_uhash_t mult;

    if (v->self_hash != -1) {
        return v->self_hash;
    }

    len = Py_SIZE(v);
    mult = _PyHASH_MULTIPLIER;

    x = 0x345678UL;
    p = v->ob_item;
    while (--len >= 0) {
        y = PyObject_Hash(*p++);
        if (y == -1) {
            return -1;
        }
        x = (x ^ (Py_uhash_t)y) * mult;
        /* the cast might truncate len; that doesn't change hash stability */
        mult += (Py_uhash_t)(82520UL + (size_t)len + (size_t)len);
    }
    x += 97531UL;
    if (x == (Py_uhash_t)-1) {
        x = (Py_uhash_t)-2;
    }
    v->self_hash = (Py_hash_t)x;
    return (Py_hash_t)x;
}

#endif


static PyObject *
record_richcompare(PyObject *v, PyObject *w, int op)
{
    Py_ssize_t i;
    Py_ssize_t vlen, wlen;
    int v_is_tuple = 0;
    int w_is_tuple = 0;
    int comp;

    if (!ApgRecord_CheckExact(v)) {
        if (!PyTuple_Check(v)) {
            Py_RETURN_NOTIMPLEMENTED;
        }
        v_is_tuple = 1;
    }

    if (!ApgRecord_CheckExact(w)) {
        if (!PyTuple_Check(w)) {
            Py_RETURN_NOTIMPLEMENTED;
        }
        w_is_tuple = 1;
    }

#define V_ITEM(i) \
    (v_is_tuple ? (PyTuple_GET_ITEM(v, i)) : (ApgRecord_GET_ITEM(v, i)))
#define W_ITEM(i) \
    (w_is_tuple ? (PyTuple_GET_ITEM(w, i)) : (ApgRecord_GET_ITEM(w, i)))

    vlen = Py_SIZE(v);
    wlen = Py_SIZE(w);

    if (op == Py_EQ && vlen != wlen) {
        /* Checking if v == w, but len(v) != len(w): return False */
        Py_RETURN_FALSE;
    }

    if (op == Py_NE && vlen != wlen) {
        /* Checking if v != w, and len(v) != len(w): return True */
        Py_RETURN_TRUE;
    }

    /* Search for the first index where items are different.
     * Note that because tuples are immutable, it's safe to reuse
     * vlen and wlen across the comparison calls.
     */
    for (i = 0; i < vlen && i < wlen; i++) {
        comp = PyObject_RichCompareBool(V_ITEM(i), W_ITEM(i), Py_EQ);
        if (comp < 0) {
            return NULL;
        }
        if (!comp) {
            break;
        }
    }

    if (i >= vlen || i >= wlen) {
        /* No more items to compare -- compare sizes */
        int cmp;
        switch (op) {
            case Py_LT: cmp = vlen <  wlen; break;
            case Py_LE: cmp = vlen <= wlen; break;
            case Py_EQ: cmp = vlen == wlen; break;
            case Py_NE: cmp = vlen != wlen; break;
            case Py_GT: cmp = vlen >  wlen; break;
            case Py_GE: cmp = vlen >= wlen; break;
            default: return NULL; /* cannot happen */
        }
        if (cmp) {
            Py_RETURN_TRUE;
        }
        else {
            Py_RETURN_FALSE;
        }
    }

    /* We have an item that differs -- shortcuts for EQ/NE */
    if (op == Py_EQ) {
        Py_RETURN_FALSE;
    }
    if (op == Py_NE) {
        Py_RETURN_TRUE;
    }

    /* Compare the final item again using the proper operator */
    return PyObject_RichCompare(V_ITEM(i), W_ITEM(i), op);

#undef V_ITEM
#undef W_ITEM
}


static PyObject *
record_item(ApgRecordObject *o, Py_ssize_t i)
{
    if (i < 0 || i >= Py_SIZE(o)) {
        PyErr_SetString(PyExc_IndexError, "record index out of range");
        return NULL;
    }
    Py_INCREF(o->ob_item[i]);
    return o->ob_item[i];
}


typedef enum item_by_name_result {
    APG_ITEM_FOUND = 0,
    APG_ERROR = -1,
    APG_ITEM_NOT_FOUND = -2
} item_by_name_result_t;


/* Lookup a record value by its name.  Return 0 on success, -2 if the
 * value was not found (with KeyError set), and -1 on all other errors.
 */
static item_by_name_result_t
record_item_by_name(ApgRecordObject *o, PyObject *item, PyObject **result)
{
    PyObject *mapped;
    PyObject *val;
    Py_ssize_t i;

    mapped = PyObject_GetItem(o->desc->mapping, item);
    if (mapped == NULL) {
        goto noitem;
    }

    if (!PyIndex_Check(mapped)) {
        Py_DECREF(mapped);
        goto error;
    }

    i = PyNumber_AsSsize_t(mapped, PyExc_IndexError);
    Py_DECREF(mapped);

    if (i < 0) {
        if (PyErr_Occurred())
            PyErr_Clear();
        goto error;
    }

    val = record_item(o, i);
    if (val == NULL) {
        PyErr_Clear();
        goto error;
    }

    *result = val;

    return APG_ITEM_FOUND;

noitem:
    PyErr_SetObject(PyExc_KeyError, item);
    return APG_ITEM_NOT_FOUND;

error:
    PyErr_SetString(PyExc_RuntimeError, "invalid record descriptor");
    return APG_ERROR;
}


static PyObject *
record_subscript(ApgRecordObject* o, PyObject* item)
{
    if (PyIndex_Check(item)) {
        Py_ssize_t i = PyNumber_AsSsize_t(item, PyExc_IndexError);
        if (i == -1 && PyErr_Occurred())
            return NULL;
        if (i < 0) {
            i += Py_SIZE(o);
        }
        return record_item(o, i);
    }
    else if (PySlice_Check(item)) {
        Py_ssize_t start, stop, step, slicelength, cur, i;
        PyObject* result;
        PyObject* it;
        PyObject **src, **dest;

        if (PySlice_GetIndicesEx(
                item,
                Py_SIZE(o),
                &start, &stop, &step, &slicelength) < 0)
        {
            return NULL;
        }

        if (slicelength <= 0) {
            return PyTuple_New(0);
        }
        else {
            result = PyTuple_New(slicelength);
            if (!result) return NULL;

            src = o->ob_item;
            dest = ((PyTupleObject *)result)->ob_item;
            for (cur = start, i = 0; i < slicelength; cur += step, i++) {
                it = src[cur];
                Py_INCREF(it);
                dest[i] = it;
            }

            return result;
        }
    }
    else {
        PyObject* result;

        if (record_item_by_name(o, item, &result) < 0)
            return NULL;
        else
            return result;
    }
}


static PyObject *
record_repr(ApgRecordObject *v)
{
    Py_ssize_t i, n;
    PyObject *keys_iter;
    _PyUnicodeWriter writer;

    n = Py_SIZE(v);
    if (n == 0) {
        return PyUnicode_FromString("<Record>");
    }

    keys_iter = PyObject_GetIter(v->desc->keys);
    if (keys_iter == NULL) {
        return NULL;
    }

    i = Py_ReprEnter((PyObject *)v);
    if (i != 0) {
        Py_DECREF(keys_iter);
        return i > 0 ? PyUnicode_FromString("<Record ...>") : NULL;
    }

    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;
    writer.min_length = 12; /* <Record a=1> */

    if (_PyUnicodeWriter_WriteASCIIString(&writer, "<Record ", 8) < 0) {
        goto error;
    }

    for (i = 0; i < n; ++i) {
        PyObject *key;
        PyObject *key_repr;
        PyObject *val_repr;

        if (i > 0) {
            if (_PyUnicodeWriter_WriteChar(&writer, ' ') < 0) {
                goto error;
            }
        }

        if (Py_EnterRecursiveCall(" while getting the repr of a record")) {
            goto error;
        }
        val_repr = PyObject_Repr(v->ob_item[i]);
        Py_LeaveRecursiveCall();
        if (val_repr == NULL) {
            goto error;
        }

        key = PyIter_Next(keys_iter);
        if (key == NULL) {
            Py_DECREF(val_repr);
            PyErr_SetString(PyExc_RuntimeError, "invalid record mapping");
            goto error;
        }

        key_repr = PyObject_Str(key);
        Py_DECREF(key);
        if (key_repr == NULL) {
            Py_DECREF(val_repr);
            goto error;
        }

        if (_PyUnicodeWriter_WriteStr(&writer, key_repr) < 0) {
            Py_DECREF(key_repr);
            Py_DECREF(val_repr);
            goto error;
        }
        Py_DECREF(key_repr);

        if (_PyUnicodeWriter_WriteChar(&writer, '=') < 0) {
            Py_DECREF(val_repr);
            goto error;
        }

        if (_PyUnicodeWriter_WriteStr(&writer, val_repr) < 0) {
            Py_DECREF(val_repr);
            goto error;
        }
        Py_DECREF(val_repr);
    }

    writer.overallocate = 0;
    if (_PyUnicodeWriter_WriteChar(&writer, '>') < 0) {
        goto error;
    }

    Py_DECREF(keys_iter);
    Py_ReprLeave((PyObject *)v);
    return _PyUnicodeWriter_Finish(&writer);

error:
    Py_DECREF(keys_iter);
    _PyUnicodeWriter_Dealloc(&writer);
    Py_ReprLeave((PyObject *)v);
    return NULL;
}



static PyObject *
record_values(PyObject *o, PyObject *args)
{
    return record_iter(o);
}


static PyObject *
record_keys(PyObject *o, PyObject *args)
{
    if (!ApgRecord_CheckExact(o)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    return PyObject_GetIter(((ApgRecordObject*)o)->desc->keys);
}


static PyObject *
record_items(PyObject *o, PyObject *args)
{
    if (!ApgRecord_CheckExact(o)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    return record_new_items_iter(o);
}


static int
record_contains(ApgRecordObject *o, PyObject *arg)
{
    if (!ApgRecord_CheckExact(o)) {
        PyErr_BadInternalCall();
        return -1;
    }

    return PySequence_Contains(o->desc->mapping, arg);
}


static PyObject *
record_get(ApgRecordObject* o, PyObject* args)
{
    PyObject *key;
    PyObject *defval = Py_None;
    PyObject *val = NULL;
    int res;

    if (!PyArg_UnpackTuple(args, "get", 1, 2, &key, &defval))
        return NULL;

    res = record_item_by_name(o, key, &val);
    if (res == APG_ITEM_NOT_FOUND) {
        PyErr_Clear();
        Py_INCREF(defval);
        val = defval;
    }

    return val;
}


static PySequenceMethods record_as_sequence = {
    (lenfunc)record_length,                          /* sq_length */
    0,                                               /* sq_concat */
    0,                                               /* sq_repeat */
    (ssizeargfunc)record_item,                       /* sq_item */
    0,                                               /* sq_slice */
    0,                                               /* sq_ass_item */
    0,                                               /* sq_ass_slice */
    (objobjproc)record_contains,                     /* sq_contains */
};


static PyMappingMethods record_as_mapping = {
    (lenfunc)record_length,                          /* mp_length */
    (binaryfunc)record_subscript,                    /* mp_subscript */
    0                                                /* mp_ass_subscript */
};


static PyMethodDef record_methods[] = {
    {"values",          (PyCFunction)record_values, METH_NOARGS},
    {"keys",            (PyCFunction)record_keys, METH_NOARGS},
    {"items",           (PyCFunction)record_items, METH_NOARGS},
    {"get",             (PyCFunction)record_get, METH_VARARGS},
    {NULL,              NULL}           /* sentinel */
};


PyTypeObject ApgRecord_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "asyncpg.Record",
    .tp_basicsize = sizeof(ApgRecordObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)record_dealloc,
    .tp_repr = (reprfunc)record_repr,
    .tp_as_sequence = &record_as_sequence,
    .tp_as_mapping = &record_as_mapping,
    .tp_hash = (hashfunc)record_hash,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE,
    .tp_traverse = (traverseproc)record_traverse,
    .tp_richcompare = record_richcompare,
    .tp_iter = record_iter,
    .tp_methods = record_methods,
    .tp_free = PyObject_GC_Del,
};


/* Record Iterator */


typedef struct {
    PyObject_HEAD
    Py_ssize_t it_index;
    ApgRecordObject *it_seq; /* Set to NULL when iterator is exhausted */
} ApgRecordIterObject;


static void
record_iter_dealloc(ApgRecordIterObject *it)
{
    PyObject_GC_UnTrack(it);
    Py_CLEAR(it->it_seq);
    PyObject_GC_Del(it);
}


static int
record_iter_traverse(ApgRecordIterObject *it, visitproc visit, void *arg)
{
    Py_VISIT(it->it_seq);
    return 0;
}


static PyObject *
record_iter_next(ApgRecordIterObject *it)
{
    ApgRecordObject *seq;
    PyObject *item;

    assert(it != NULL);
    seq = it->it_seq;
    if (seq == NULL)
        return NULL;
    assert(ApgRecord_CheckExact(seq));

    if (it->it_index < Py_SIZE(seq)) {
        item = ApgRecord_GET_ITEM(seq, it->it_index);
        ++it->it_index;
        Py_INCREF(item);
        return item;
    }

    it->it_seq = NULL;
    Py_DECREF(seq);
    return NULL;
}


static PyObject *
record_iter_len(ApgRecordIterObject *it)
{
    Py_ssize_t len = 0;
    if (it->it_seq) {
        len = Py_SIZE(it->it_seq) - it->it_index;
    }
    return PyLong_FromSsize_t(len);
}


PyDoc_STRVAR(record_iter_len_doc,
             "Private method returning an estimate of len(list(it)).");


static PyMethodDef record_iter_methods[] = {
    {"__length_hint__", (PyCFunction)record_iter_len, METH_NOARGS,
        record_iter_len_doc},
    {NULL,              NULL}           /* sentinel */
};


PyTypeObject ApgRecordIter_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "RecordIterator",
    .tp_basicsize = sizeof(ApgRecordIterObject),
    .tp_dealloc = (destructor)record_iter_dealloc,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)record_iter_traverse,
    .tp_iter = PyObject_SelfIter,
    .tp_iternext = (iternextfunc)record_iter_next,
    .tp_methods = record_iter_methods,
};


static PyObject *
record_iter(PyObject *seq)
{
    ApgRecordIterObject *it;

    if (!ApgRecord_CheckExact(seq)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    it = PyObject_GC_New(ApgRecordIterObject, &ApgRecordIter_Type);
    if (it == NULL)
        return NULL;
    it->it_index = 0;
    Py_INCREF(seq);
    it->it_seq = (ApgRecordObject *)seq;
    PyObject_GC_Track(it);
    return (PyObject *)it;
}


/* Record Items Iterator */


typedef struct {
    PyObject_HEAD
    Py_ssize_t it_index;
    PyObject *it_key_iter;
    ApgRecordObject *it_seq; /* Set to NULL when iterator is exhausted */
} ApgRecordItemsObject;


static void
record_items_dealloc(ApgRecordItemsObject *it)
{
    PyObject_GC_UnTrack(it);
    Py_CLEAR(it->it_key_iter);
    Py_CLEAR(it->it_seq);
    PyObject_GC_Del(it);
}


static int
record_items_traverse(ApgRecordItemsObject *it, visitproc visit, void *arg)
{
    Py_VISIT(it->it_key_iter);
    Py_VISIT(it->it_seq);
    return 0;
}


static PyObject *
record_items_next(ApgRecordItemsObject *it)
{
    ApgRecordObject *seq;
    PyObject *key;
    PyObject *val;
    PyObject *tup;

    assert(it != NULL);
    seq = it->it_seq;
    if (seq == NULL) {
        return NULL;
    }
    assert(ApgRecord_CheckExact(seq));
    assert(it->it_key_iter != NULL);

    key = PyIter_Next(it->it_key_iter);
    if (key == NULL) {
        /* likely it_key_iter had less items than seq has values */
        goto exhausted;
    }

    if (it->it_index < Py_SIZE(seq)) {
        val = ApgRecord_GET_ITEM(seq, it->it_index);
        ++it->it_index;
        Py_INCREF(val);
    }
    else {
        /* it_key_iter had more items than seq has values */
        Py_DECREF(key);
        goto exhausted;
    }

    tup = PyTuple_New(2);
    if (tup == NULL) {
        Py_DECREF(val);
        Py_DECREF(key);
        goto exhausted;
    }

    PyTuple_SET_ITEM(tup, 0, key);
    PyTuple_SET_ITEM(tup, 1, val);
    return tup;

exhausted:
    Py_CLEAR(it->it_key_iter);
    Py_CLEAR(it->it_seq);
    return NULL;
}


static PyObject *
record_items_len(ApgRecordItemsObject *it)
{
    Py_ssize_t len = 0;
    if (it->it_seq) {
        len = Py_SIZE(it->it_seq) - it->it_index;
    }
    return PyLong_FromSsize_t(len);
}


PyDoc_STRVAR(record_items_len_doc,
             "Private method returning an estimate of len(list(it())).");


static PyMethodDef record_items_methods[] = {
    {"__length_hint__", (PyCFunction)record_items_len, METH_NOARGS,
        record_items_len_doc},
    {NULL,              NULL}           /* sentinel */
};


PyTypeObject ApgRecordItems_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "RecordItemsIterator",
    .tp_basicsize = sizeof(ApgRecordItemsObject),
    .tp_dealloc = (destructor)record_items_dealloc,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)record_items_traverse,
    .tp_iter = PyObject_SelfIter,
    .tp_iternext = (iternextfunc)record_items_next,
    .tp_methods = record_items_methods,
};


static PyObject *
record_new_items_iter(PyObject *seq)
{
    ApgRecordItemsObject *it;
    PyObject *key_iter;

    if (!ApgRecord_CheckExact(seq)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    key_iter = PyObject_GetIter(((ApgRecordObject*)seq)->desc->keys);
    if (key_iter == NULL) {
        return NULL;
    }

    it = PyObject_GC_New(ApgRecordItemsObject, &ApgRecordItems_Type);
    if (it == NULL)
        return NULL;

    it->it_key_iter = key_iter;
    it->it_index = 0;
    Py_INCREF(seq);
    it->it_seq = (ApgRecordObject *)seq;
    PyObject_GC_Track(it);

    return (PyObject *)it;
}


PyTypeObject *
ApgRecord_InitTypes(void)
{
    if (PyType_Ready(&ApgRecord_Type) < 0) {
        return NULL;
    }

    if (PyType_Ready(&ApgRecordDesc_Type) < 0) {
        return NULL;
    }

    if (PyType_Ready(&ApgRecordIter_Type) < 0) {
        return NULL;
    }

    if (PyType_Ready(&ApgRecordItems_Type) < 0) {
        return NULL;
    }

    return &ApgRecord_Type;
}


/* ----------------- */


static void
record_desc_dealloc(ApgRecordDescObject *o)
{
    PyObject_GC_UnTrack(o);
    Py_CLEAR(o->mapping);
    Py_CLEAR(o->keys);
    PyObject_GC_Del(o);
}


static int
record_desc_traverse(ApgRecordDescObject *o, visitproc visit, void *arg)
{
    Py_VISIT(o->mapping);
    Py_VISIT(o->keys);
    return 0;
}


PyTypeObject ApgRecordDesc_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "RecordDescriptor",
    .tp_basicsize = sizeof(ApgRecordDescObject),
    .tp_dealloc = (destructor)record_desc_dealloc,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)record_desc_traverse,
    .tp_iter = PyObject_SelfIter,
};


PyObject *
ApgRecordDesc_New(PyObject *mapping, PyObject *keys)
{
    ApgRecordDescObject *o;

    if (!mapping || !keys || !PyTuple_CheckExact(keys)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    o = PyObject_GC_New(ApgRecordDescObject, &ApgRecordDesc_Type);
    if (o == NULL) {
        return NULL;
    }

    Py_INCREF(mapping);
    o->mapping = mapping;

    Py_INCREF(keys);
    o->keys = keys;

    PyObject_GC_Track(o);
    return (PyObject *) o;
}
