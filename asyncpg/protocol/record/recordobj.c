/* Big parts of this file are copied (with modifications) from
   CPython/Objects/tupleobject.c.

   Portions Copyright (c) PSF (and other CPython copyright holders).
   Portions Copyright (c) 2016-present MagicStack Inc.
   License: PSFL v2; see CPython/LICENSE for details.
*/

#include <stdint.h>
#include <Python.h>
#include "pythoncapi_compat.h"
#include "pythoncapi_compat_extras.h"

#include "recordobj.h"

#ifndef _PyCFunction_CAST
#define _PyCFunction_CAST(func) ((PyCFunction)(void (*)(void))(func))
#endif

static size_t ApgRecord_MAXSIZE =
    (((size_t)PY_SSIZE_T_MAX - sizeof(ApgRecordObject) - sizeof(PyObject *)) /
     sizeof(PyObject *));

/* Largest record to save on free list */
#define ApgRecord_MAXSAVESIZE 20

/* Maximum number of records of each size to save */
#define ApgRecord_MAXFREELIST 2000

typedef struct {
    ApgRecordObject *freelist[ApgRecord_MAXSAVESIZE];
    int numfree[ApgRecord_MAXSAVESIZE];
} record_freelist_state;

typedef struct {
    PyTypeObject *ApgRecord_Type;
    PyTypeObject *ApgRecordDesc_Type;
    PyTypeObject *ApgRecordIter_Type;
    PyTypeObject *ApgRecordItems_Type;

    Py_tss_t freelist_key;  // TSS key for per-thread record_freelist_state
} record_module_state;

static inline record_module_state *
get_module_state(PyObject *module)
{
    void *state = PyModule_GetState(module);
    if (state == NULL) {
        PyErr_SetString(PyExc_SystemError, "failed to get record module state");
        return NULL;
    }
    return (record_module_state *)state;
}

static inline record_module_state *
get_module_state_from_type(PyTypeObject *type)
{
    void *state = PyType_GetModuleState(type);
    if (state != NULL) {
        return (record_module_state *)state;
    }

    PyErr_Format(PyExc_SystemError, "could not get record module state from '%.100s'",
                 type->tp_name);
    return NULL;
}

static struct PyModuleDef _recordmodule;

static inline record_module_state *
find_module_state_by_def(PyTypeObject *type)
{
    PyObject *mod = PyType_GetModuleByDef(type, &_recordmodule);
    if (mod == NULL)
        return NULL;
    return get_module_state(mod);
}

static inline record_freelist_state *
get_freelist_state(record_module_state *state)
{
    record_freelist_state *freelist;

    freelist = (record_freelist_state *)PyThread_tss_get(&state->freelist_key);
    if (freelist == NULL) {
        freelist = (record_freelist_state *)PyMem_Calloc(
            1, sizeof(record_freelist_state));
        if (freelist == NULL) {
            PyErr_NoMemory();
            return NULL;
        }
        if (PyThread_tss_set(&state->freelist_key, (void *)freelist) != 0) {
            PyMem_Free(freelist);
            PyErr_SetString(
                PyExc_SystemError, "failed to set thread-specific data");
            return NULL;
        }
    }
    return freelist;
}

PyObject *
make_record(PyTypeObject *type, PyObject *desc, Py_ssize_t size,
            record_module_state *state)
{
    ApgRecordObject *o;
    Py_ssize_t i;
    int need_gc_track = 0;

    if (size < 0 || desc == NULL ||
        Py_TYPE(desc) != state->ApgRecordDesc_Type) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (type == state->ApgRecord_Type) {
        record_freelist_state *freelist = NULL;

        if (size < ApgRecord_MAXSAVESIZE) {
            freelist = get_freelist_state(state);
            if (freelist != NULL && freelist->freelist[size] != NULL) {
                o = freelist->freelist[size];
                freelist->freelist[size] = (ApgRecordObject *)o->ob_item[0];
                freelist->numfree[size]--;
                _Py_NewReference((PyObject *)o);
            }
            else {
                freelist = NULL;
            }
        }

        if (freelist == NULL) {
            if ((size_t)size > ApgRecord_MAXSIZE) {
                return PyErr_NoMemory();
            }
            o = PyObject_GC_NewVar(ApgRecordObject, state->ApgRecord_Type, size);
            if (o == NULL) {
                return NULL;
            }
        }

        need_gc_track = 1;
    }
    else {
        assert(PyType_IsSubtype(type, state->ApgRecord_Type));

        if ((size_t)size > ApgRecord_MAXSIZE) {
            return PyErr_NoMemory();
        }
        o = (ApgRecordObject *)type->tp_alloc(type, size);
        if (!PyObject_GC_IsTracked((PyObject *)o)) {
            PyErr_SetString(PyExc_TypeError, "record subclass is not tracked by GC");
            return NULL;
        }
    }

    for (i = 0; i < size; i++) {
        o->ob_item[i] = NULL;
    }

    Py_INCREF(desc);
    o->desc = (ApgRecordDescObject *)desc;
    o->self_hash = -1;
    if (need_gc_track) {
        PyObject_GC_Track(o);
    }
    return (PyObject *)o;
}

static void
record_dealloc(PyObject *self)
{
    ApgRecordObject *o = (ApgRecordObject *)self;
    Py_ssize_t i;
    Py_ssize_t len = Py_SIZE(o);
    record_module_state *state;

    state = find_module_state_by_def(Py_TYPE(o));
    if (state == NULL) {
        return;
    }

    PyObject_GC_UnTrack(o);

    o->self_hash = -1;

    Py_CLEAR(o->desc);

    Py_TRASHCAN_BEGIN(o, record_dealloc)

    i = len;
    while (--i >= 0) {
        Py_XDECREF(o->ob_item[i]);
    }

    if (len < ApgRecord_MAXSAVESIZE && Py_TYPE(o) == state->ApgRecord_Type) {
        record_freelist_state *freelist = get_freelist_state(state);
        if (freelist != NULL && freelist->numfree[len] < ApgRecord_MAXFREELIST) {
            o->ob_item[0] = (PyObject *)freelist->freelist[len];
            freelist->numfree[len]++;
            freelist->freelist[len] = o;
        }
        else {
            Py_TYPE(o)->tp_free((PyObject *)o);
        }
    }
    else {
        Py_TYPE(o)->tp_free((PyObject *)o);
    }

    Py_TRASHCAN_END
}

static int
record_traverse(PyObject *self, visitproc visit, void *arg)
{
    ApgRecordObject *o = (ApgRecordObject *)self;
    for (Py_ssize_t i = Py_SIZE(o); --i >= 0;) {
        Py_VISIT(o->ob_item[i]);
    }
    return 0;
}

/* Below are the official constants from the xxHash specification. Optimizing
   compilers should emit a single "rotate" instruction for the
   _PyTuple_HASH_XXROTATE() expansion. If that doesn't happen for some important
   platform, the macro could be changed to expand to a platform-specific rotate
   spelling instead.
*/
#if SIZEOF_PY_UHASH_T > 4
#define _ApgRecord_HASH_XXPRIME_1 ((Py_uhash_t)11400714785074694791ULL)
#define _ApgRecord_HASH_XXPRIME_2 ((Py_uhash_t)14029467366897019727ULL)
#define _ApgRecord_HASH_XXPRIME_5 ((Py_uhash_t)2870177450012600261ULL)
#define _ApgRecord_HASH_XXROTATE(x) ((x << 31) | (x >> 33)) /* Rotate left 31 bits */
#else
#define _ApgRecord_HASH_XXPRIME_1 ((Py_uhash_t)2654435761UL)
#define _ApgRecord_HASH_XXPRIME_2 ((Py_uhash_t)2246822519UL)
#define _ApgRecord_HASH_XXPRIME_5 ((Py_uhash_t)374761393UL)
#define _ApgRecord_HASH_XXROTATE(x) ((x << 13) | (x >> 19)) /* Rotate left 13 bits */
#endif

static Py_hash_t
record_hash(PyObject *op)
{
    ApgRecordObject *v = (ApgRecordObject *)op;
    Py_uhash_t acc;
    Py_ssize_t len = Py_SIZE(v);
    PyObject **item = v->ob_item;
    acc = _ApgRecord_HASH_XXPRIME_5;
    for (Py_ssize_t i = 0; i < len; i++) {
        Py_uhash_t lane = (Py_uhash_t)PyObject_Hash(item[i]);
        if (lane == (Py_uhash_t)-1) {
            return -1;
        }
        acc += lane * _ApgRecord_HASH_XXPRIME_2;
        acc = _ApgRecord_HASH_XXROTATE(acc);
        acc *= _ApgRecord_HASH_XXPRIME_1;
    }

    /* Add input length, mangled to keep the historical value of hash(()). */
    acc += (Py_uhash_t)len ^ (_ApgRecord_HASH_XXPRIME_5 ^ 3527539UL);

    if (acc == (Py_uhash_t)-1) {
        acc = 1546275796;
    }

    return (Py_hash_t)acc;
}

static Py_ssize_t
record_length(PyObject *self)
{
    ApgRecordObject *a = (ApgRecordObject *)self;
    return Py_SIZE(a);
}

static int
record_contains(PyObject *self, PyObject *el)
{
    ApgRecordObject *a = (ApgRecordObject *)self;
    if (a->desc == NULL || a->desc->keys == NULL) {
        return 0;
    }
    return PySequence_Contains(a->desc->keys, el);
}

static PyObject *
record_item(ApgRecordObject *op, Py_ssize_t i)
{
    ApgRecordObject *a = (ApgRecordObject *)op;
    if (i < 0 || i >= Py_SIZE(a)) {
        PyErr_SetString(PyExc_IndexError, "record index out of range");
        return NULL;
    }
    return Py_NewRef(a->ob_item[i]);
}

static PyObject *
record_richcompare(PyObject *v, PyObject *w, int op)
{
    Py_ssize_t i;
    Py_ssize_t vlen, wlen;
    int v_is_tuple = 0;
    int w_is_tuple = 0;
    int v_is_record = 0;
    int w_is_record = 0;
    int comp;
    PyTypeObject *v_type = Py_TYPE(v);
    PyTypeObject *w_type = Py_TYPE(w);

    record_module_state *state;

    state = find_module_state_by_def(v_type);
    if (state == NULL) {
        PyErr_Clear();
        state = find_module_state_by_def(w_type);
    }
    if (PyTuple_Check(v)) {
        v_is_tuple = 1;
    }
    else if (v_type == state->ApgRecord_Type) {
        v_is_record = 1;
    }
    else if (!PyObject_TypeCheck(v, state->ApgRecord_Type)) {
        Py_RETURN_NOTIMPLEMENTED;
    }

    if (PyTuple_Check(w)) {
        w_is_tuple = 1;
    }
    else if (w_type == state->ApgRecord_Type) {
        w_is_record = 1;
    }
    else if (!PyObject_TypeCheck(w, state->ApgRecord_Type)) {
        Py_RETURN_NOTIMPLEMENTED;
    }

#define V_ITEM(i)                        \
    (v_is_tuple ? PyTuple_GET_ITEM(v, i) \
                : (v_is_record ? ApgRecord_GET_ITEM(v, i) : PySequence_GetItem(v, i)))
#define W_ITEM(i)                        \
    (w_is_tuple ? PyTuple_GET_ITEM(w, i) \
                : (w_is_record ? ApgRecord_GET_ITEM(w, i) : PySequence_GetItem(w, i)))

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
            case Py_LT:
                cmp = vlen < wlen;
                break;
            case Py_LE:
                cmp = vlen <= wlen;
                break;
            case Py_EQ:
                cmp = vlen == wlen;
                break;
            case Py_NE:
                cmp = vlen != wlen;
                break;
            case Py_GT:
                cmp = vlen > wlen;
                break;
            case Py_GE:
                cmp = vlen >= wlen;
                break;
            default:
                return NULL; /* cannot happen */
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
record_subscript(PyObject *op, PyObject *item)
{
    ApgRecordObject *self = (ApgRecordObject *)op;

    if (PyIndex_Check(item)) {
        Py_ssize_t i = PyNumber_AsSsize_t(item, PyExc_IndexError);
        if (i == -1 && PyErr_Occurred())
            return NULL;
        if (i < 0) {
            i += Py_SIZE(self);
        }
        return record_item(self, i);
    }
    else if (PySlice_Check(item)) {
        Py_ssize_t start, stop, step, cur, slicelength, i;
        PyObject *it;
        PyObject **src, **dest;

        if (PySlice_Unpack(item, &start, &stop, &step) < 0) {
            return NULL;
        }
        slicelength = PySlice_AdjustIndices(Py_SIZE(self), &start, &stop, step);

        if (slicelength <= 0) {
            return PyTuple_New(0);
        }
        else if (start == 0 && step == 1 && slicelength == Py_SIZE(self) &&
                 PyTuple_CheckExact(self)) {
            return Py_NewRef(self);
        }
        else {
            PyTupleObject *result = (PyTupleObject *)PyTuple_New(slicelength);
            if (!result)
                return NULL;

            src = self->ob_item;
            dest = result->ob_item;
            for (cur = start, i = 0; i < slicelength; cur += step, i++) {
                it = Py_NewRef(src[cur]);
                dest[i] = it;
            }

            return (PyObject *)result;
        }
    }
    else {
        PyObject *result;

        if (record_item_by_name(self, item, &result) < 0)
            return NULL;
        else
            return result;
    }
}

static const char *
get_typename(PyTypeObject *type)
{
    assert(type->tp_name != NULL);
    const char *s = strrchr(type->tp_name, '.');
    if (s == NULL) {
        s = type->tp_name;
    }
    else {
        s++;
    }
    return s;
}

static PyObject *
record_repr(PyObject *self)
{
    ApgRecordObject *v = (ApgRecordObject *)self;
    Py_ssize_t i, n;
    PyObject *keys_iter;
    PyUnicodeWriter *writer;

    n = Py_SIZE(v);
    if (n == 0) {
        return PyUnicode_FromFormat("<%s>", get_typename(Py_TYPE(v)));
    }

    keys_iter = PyObject_GetIter(v->desc->keys);
    if (keys_iter == NULL) {
        return NULL;
    }

    i = Py_ReprEnter((PyObject *)v);
    if (i != 0) {
        Py_DECREF(keys_iter);
        if (i > 0) {
            return PyUnicode_FromFormat("<%s ...>", get_typename(Py_TYPE(v)));
        }
        return NULL;
    }
    writer = PyUnicodeWriter_Create(12); /* <Record a=1> */

    if (PyUnicodeWriter_Format(writer, "<%s ", get_typename(Py_TYPE(v))) < 0) {
        goto error;
    }

    for (i = 0; i < n; ++i) {
        int res;
        PyObject *key;

        if (i > 0)
            if (PyUnicodeWriter_WriteChar(writer, ' ') < 0)
                goto error;

        key = PyIter_Next(keys_iter);
        if (key == NULL) {
            PyErr_SetString(PyExc_RuntimeError, "invalid record mapping");
            goto error;
        }

        res = PyUnicodeWriter_WriteStr(writer, key);
        Py_DECREF(key);
        if (res < 0)
            goto error;

        if (PyUnicodeWriter_WriteChar(writer, '=') < 0)
            goto error;

        if (Py_EnterRecursiveCall(" while getting the repr of a record"))
            goto error;
        res = PyUnicodeWriter_WriteRepr(writer, v->ob_item[i]);
        Py_LeaveRecursiveCall();
        if (res < 0)
            goto error;
    }

    if (PyUnicodeWriter_WriteChar(writer, '>') < 0)
        goto error;

    Py_DECREF(keys_iter);
    Py_ReprLeave((PyObject *)v);
    return PyUnicodeWriter_Finish(writer);

error:
    Py_DECREF(keys_iter);
    PyUnicodeWriter_Discard(writer);
    Py_ReprLeave((PyObject *)v);
    return NULL;
}

static PyObject *
record_new_iter(ApgRecordObject *, const record_module_state *);

static PyObject *
record_iter(PyObject *seq)
{
    ApgRecordObject *r = (ApgRecordObject *)seq;
    record_module_state *state;

    state = find_module_state_by_def(Py_TYPE(seq));
    if (state == NULL) {
        return NULL;
    }

    return record_new_iter(r, state);
}

static PyObject *
record_values(PyObject *self, PyTypeObject *defcls, PyObject *const *args,
              size_t nargsf, PyObject *kwnames)
{
    ApgRecordObject *r = (ApgRecordObject *)self;
    record_module_state *state = get_module_state_from_type(defcls);

    if (state == NULL)
        return NULL;

    return record_new_iter(r, state);
}

static PyObject *
record_keys(PyObject *self, PyTypeObject *defcls, PyObject *const *args,
            size_t nargsf, PyObject *kwnames)
{
    ApgRecordObject *r = (ApgRecordObject *)self;
    return PyObject_GetIter(r->desc->keys);
}

static PyObject *
record_new_items_iter(ApgRecordObject *, const record_module_state *);

static PyObject *
record_items(PyObject *self, PyTypeObject *defcls, PyObject *const *args,
             size_t nargsf, PyObject *kwnames)
{
    ApgRecordObject *r = (ApgRecordObject *)self;
    record_module_state *state = get_module_state_from_type(defcls);

    if (state == NULL)
        return NULL;

    return record_new_items_iter(r, state);
}

static PyObject *
record_get(PyObject *self, PyTypeObject *defcls, PyObject *const *args,
           size_t nargsf, PyObject *kwnames)
{
    Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);
    PyObject *key;
    PyObject *defval = Py_None;
    PyObject *val = NULL;
    int res;

    if (nargs == 2) {
        key = args[0];
        defval = args[1];
    } else if (nargs == 1) {
        key = args[0];
    } else {
        PyErr_Format(PyExc_TypeError,
                     "Record.get() expected 1 or 2 arguments, got %zd",
                     nargs);
    }

    if (kwnames != NULL && PyTuple_GET_SIZE(kwnames) != 0) {
        PyErr_SetString(PyExc_TypeError, "Record.get() takes no keyword arguments");
        return NULL;
    }

    res = record_item_by_name((ApgRecordObject *)self, key, &val);
    if (res == APG_ITEM_NOT_FOUND) {
        PyErr_Clear();
        Py_INCREF(defval);
        val = defval;
    }

    return val;
}

static PyObject *
record_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    record_module_state *state;

    state = get_module_state_from_type(type);
    if (state == NULL) {
        return NULL;
    }

    if (type == state->ApgRecord_Type) {
        PyErr_Format(PyExc_TypeError, "cannot create '%.100s' instances", type->tp_name);
        return NULL;
    }

    /* For subclasses, use the default allocation */
    return type->tp_alloc(type, 0);
}

static PyMethodDef record_methods[] = {
    {"values", _PyCFunction_CAST(record_values), METH_METHOD | METH_FASTCALL | METH_KEYWORDS},
    {"keys", _PyCFunction_CAST(record_keys), METH_METHOD | METH_FASTCALL | METH_KEYWORDS},
    {"items", _PyCFunction_CAST(record_items), METH_METHOD | METH_FASTCALL | METH_KEYWORDS},
    {"get", _PyCFunction_CAST(record_get), METH_METHOD | METH_FASTCALL | METH_KEYWORDS},
    {NULL, NULL} /* sentinel */
};

static PyType_Slot ApgRecord_TypeSlots[] = {
    {Py_tp_dealloc, record_dealloc},
    {Py_tp_repr, record_repr},
    {Py_tp_hash, record_hash},
    {Py_tp_getattro, PyObject_GenericGetAttr},
    {Py_tp_traverse, record_traverse},
    {Py_tp_richcompare, record_richcompare},
    {Py_tp_iter, record_iter},
    {Py_tp_methods, record_methods},
    {Py_tp_new, record_new},
    {Py_tp_free, PyObject_GC_Del},
    {Py_sq_length, record_length},
    {Py_sq_item, record_item},
    {Py_sq_contains, record_contains},
    {Py_mp_length, record_length},
    {Py_mp_subscript, record_subscript},
    {0, NULL},
};

#ifndef Py_TPFLAGS_IMMUTABLETYPE
#define Py_TPFLAGS_IMMUTABLETYPE 0
#endif

static PyType_Spec ApgRecord_TypeSpec = {
    .name = "asyncpg.protocol.record.Record",
    .basicsize = sizeof(ApgRecordObject) - sizeof(PyObject *),
    .itemsize = sizeof(PyObject *),
    .flags = (Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE |
              Py_TPFLAGS_IMMUTABLETYPE),
    .slots = ApgRecord_TypeSlots,
};

/* Record Iterator */

typedef struct {
    PyObject_HEAD Py_ssize_t it_index;
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

PyDoc_STRVAR(record_iter_len_doc, "Private method returning an estimate of len(list(it)).");

static PyMethodDef record_iter_methods[] = {
    {"__length_hint__", (PyCFunction)record_iter_len, METH_NOARGS, record_iter_len_doc},
    {NULL, NULL} /* sentinel */
};

static PyType_Slot ApgRecordIter_TypeSlots[] = {
    {Py_tp_dealloc, (destructor)record_iter_dealloc},
    {Py_tp_getattro, PyObject_GenericGetAttr},
    {Py_tp_traverse, (traverseproc)record_iter_traverse},
    {Py_tp_iter, PyObject_SelfIter},
    {Py_tp_iternext, (iternextfunc)record_iter_next},
    {Py_tp_methods, record_iter_methods},
    {0, NULL},
};

static PyType_Spec ApgRecordIter_TypeSpec = {
    .name = "asyncpg.protocol.record.RecordIterator",
    .basicsize = sizeof(ApgRecordIterObject),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = ApgRecordIter_TypeSlots,
};

static PyObject *
record_new_iter(ApgRecordObject *r, const record_module_state *state)
{
    ApgRecordIterObject *it;
    it = PyObject_GC_New(ApgRecordIterObject, state->ApgRecordIter_Type);
    if (it == NULL)
        return NULL;
    it->it_index = 0;
    Py_INCREF(r);
    it->it_seq = r;
    PyObject_GC_Track(it);
    return (PyObject *)it;
}

/* Record Items Iterator */

typedef struct {
    PyObject_HEAD Py_ssize_t it_index;
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

PyDoc_STRVAR(record_items_len_doc, "Private method returning an estimate of len(list(it())).");

static PyMethodDef record_items_methods[] = {
    {"__length_hint__", (PyCFunction)record_items_len, METH_NOARGS, record_items_len_doc},
    {NULL, NULL} /* sentinel */
};

static PyType_Slot ApgRecordItems_TypeSlots[] = {
    {Py_tp_dealloc, (destructor)record_items_dealloc},
    {Py_tp_getattro, PyObject_GenericGetAttr},
    {Py_tp_traverse, (traverseproc)record_items_traverse},
    {Py_tp_iter, PyObject_SelfIter},
    {Py_tp_iternext, (iternextfunc)record_items_next},
    {Py_tp_methods, record_items_methods},
    {0, NULL},
};

static PyType_Spec ApgRecordItems_TypeSpec = {
    .name = "asyncpg.protocol.record.RecordItemsIterator",
    .basicsize = sizeof(ApgRecordItemsObject),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = ApgRecordItems_TypeSlots,
};

static PyObject *
record_new_items_iter(ApgRecordObject *r, const record_module_state *state)
{
    ApgRecordItemsObject *it;
    PyObject *key_iter;

    key_iter = PyObject_GetIter(r->desc->keys);
    if (key_iter == NULL)
        return NULL;

    it = PyObject_GC_New(ApgRecordItemsObject, state->ApgRecordItems_Type);
    if (it == NULL) {
        Py_DECREF(key_iter);
        return NULL;
    }

    it->it_key_iter = key_iter;
    it->it_index = 0;
    Py_INCREF(r);
    it->it_seq = r;
    PyObject_GC_Track(it);

    return (PyObject *)it;
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

static PyObject *
record_desc_vectorcall(PyObject *type, PyObject *const *args, size_t nargsf,
                       PyObject *kwnames)
{
    PyObject *mapping;
    PyObject *keys;
    ApgRecordDescObject *o;
    Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);

    if (kwnames != NULL && PyTuple_GET_SIZE(kwnames) != 0) {
        PyErr_SetString(PyExc_TypeError, "RecordDescriptor() takes no keyword arguments");
        return NULL;
    }

    if (nargs != 2) {
        PyErr_Format(PyExc_TypeError,
                     "RecordDescriptor() takes exactly 2 arguments (%zd given)", nargs);
        return NULL;
    }

    mapping = args[0];
    keys = args[1];

    if (!PyTuple_CheckExact(keys)) {
        PyErr_SetString(PyExc_TypeError, "keys must be a tuple");
        return NULL;
    }

    o = PyObject_GC_New(ApgRecordDescObject, (PyTypeObject *)type);
    if (o == NULL) {
        return NULL;
    }

    Py_INCREF(mapping);
    o->mapping = mapping;

    Py_INCREF(keys);
    o->keys = keys;

    PyObject_GC_Track(o);
    return (PyObject *)o;
}

/* Fallback wrapper for when there is no vectorcall support */
static PyObject *
record_desc_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    PyObject *const *args_array;
    size_t nargsf;
    PyObject *kwnames = NULL;

    if (kwargs != NULL && PyDict_GET_SIZE(kwargs) != 0) {
        PyErr_SetString(PyExc_TypeError,
                        "RecordDescriptor() takes no keyword arguments");
        return NULL;
    }

    if (!PyTuple_Check(args)) {
        PyErr_SetString(PyExc_TypeError,
                        "args must be a tuple");
        return NULL;
    }

    nargsf = (size_t)PyTuple_GET_SIZE(args);
    args_array = &PyTuple_GET_ITEM(args, 0);

    return record_desc_vectorcall((PyObject *)type, args_array, nargsf, kwnames);
}

static PyObject *
record_desc_make_record(PyObject *desc, PyTypeObject *desc_type,
                        PyObject *const *args, Py_ssize_t nargs,
                        PyObject *kwnames)
{
    PyObject *type_obj;
    Py_ssize_t size;
    record_module_state *state = get_module_state_from_type(desc_type);

    if (state == NULL) {
        return NULL;
    }

    if (nargs != 2) {
        PyErr_Format(PyExc_TypeError,
                     "RecordDescriptor.make_record() takes exactly 2 arguments (%zd given)",
                     nargs);
        return NULL;
    }

    if (kwnames != NULL && PyTuple_GET_SIZE(kwnames) != 0) {
        PyErr_SetString(PyExc_TypeError,
                        "RecordDescriptor.make_record() takes no keyword arguments");
        return NULL;
    }

    type_obj = args[0];
    size = PyLong_AsSsize_t(args[1]);
    if (size == -1 && PyErr_Occurred()) {
        return NULL;
    }

    if (!PyType_Check(type_obj)) {
        PyErr_SetString(PyExc_TypeError,
                        "RecordDescriptor.make_record(): first argument must be a type");
        return NULL;
    }

    return make_record((PyTypeObject *)type_obj, desc, size, state);
}

static PyMethodDef record_desc_methods[] = {
    {"make_record", _PyCFunction_CAST(record_desc_make_record),
     METH_FASTCALL | METH_METHOD | METH_KEYWORDS},
    {NULL, NULL} /* sentinel */
};

static PyType_Slot ApgRecordDesc_TypeSlots[] = {
#ifdef Py_tp_vectorcall
    {Py_tp_vectorcall, (vectorcallfunc)record_desc_vectorcall},
#endif
    {Py_tp_new, (newfunc)record_desc_new},
    {Py_tp_dealloc, (destructor)record_desc_dealloc},
    {Py_tp_getattro, PyObject_GenericGetAttr},
    {Py_tp_traverse, (traverseproc)record_desc_traverse},
    {Py_tp_methods, record_desc_methods},
    {0, NULL},
};

static PyType_Spec ApgRecordDesc_TypeSpec = {
    .name = "asyncpg.protocol.record.RecordDescriptor",
    .basicsize = sizeof(ApgRecordDescObject),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_IMMUTABLETYPE,
    .slots = ApgRecordDesc_TypeSlots,
};

/*
 * Module init
 */

static PyMethodDef record_module_methods[] = {{NULL, NULL, 0, NULL}};

static int
record_module_exec(PyObject *module)
{
    record_module_state *state = get_module_state(module);
    if (state == NULL) {
        return -1;
    }

    if (PyThread_tss_create(&state->freelist_key) != 0) {
        PyErr_SetString(
            PyExc_SystemError,
            "failed to create TSS key for record freelist");
        return -1;
    }

#define CREATE_TYPE(m, tp, spec)                                      \
    do {                                                              \
        tp = (PyTypeObject *)PyType_FromModuleAndSpec(m, spec, NULL); \
        if (tp == NULL)                                               \
            goto error;                                               \
        if (PyModule_AddType(m, tp) < 0)                              \
            goto error;                                               \
    } while (0)

    CREATE_TYPE(module, state->ApgRecord_Type, &ApgRecord_TypeSpec);
    CREATE_TYPE(module, state->ApgRecordDesc_Type, &ApgRecordDesc_TypeSpec);
    CREATE_TYPE(module, state->ApgRecordIter_Type, &ApgRecordIter_TypeSpec);
    CREATE_TYPE(module, state->ApgRecordItems_Type, &ApgRecordItems_TypeSpec);

#undef CREATE_TYPE

    return 0;

error:
    Py_CLEAR(state->ApgRecord_Type);
    Py_CLEAR(state->ApgRecordDesc_Type);
    Py_CLEAR(state->ApgRecordIter_Type);
    Py_CLEAR(state->ApgRecordItems_Type);
    return -1;
}

static int
record_module_traverse(PyObject *module, visitproc visit, void *arg)
{
    record_module_state *state = get_module_state(module);
    if (state == NULL) {
        return 0;
    }

    Py_VISIT(state->ApgRecord_Type);
    Py_VISIT(state->ApgRecordDesc_Type);
    Py_VISIT(state->ApgRecordIter_Type);
    Py_VISIT(state->ApgRecordItems_Type);

    return 0;
}

static int
record_module_clear(PyObject *module)
{
    record_module_state *state = get_module_state(module);
    if (state == NULL) {
        return 0;
    }

    if (PyThread_tss_is_created(&state->freelist_key)) {
        record_freelist_state *freelist =
            (record_freelist_state *)PyThread_tss_get(&state->freelist_key);
        if (freelist != NULL) {
            for (int i = 0; i < ApgRecord_MAXSAVESIZE; i++) {
                ApgRecordObject *op = freelist->freelist[i];
                while (op != NULL) {
                    ApgRecordObject *next = (ApgRecordObject *)(op->ob_item[0]);
                    PyObject_GC_Del(op);
                    op = next;
                }
                freelist->freelist[i] = NULL;
                freelist->numfree[i] = 0;
            }
            PyMem_Free(freelist);
            PyThread_tss_set(&state->freelist_key, NULL);
        }

        PyThread_tss_delete(&state->freelist_key);
    }

    Py_CLEAR(state->ApgRecord_Type);
    Py_CLEAR(state->ApgRecordDesc_Type);
    Py_CLEAR(state->ApgRecordIter_Type);
    Py_CLEAR(state->ApgRecordItems_Type);

    return 0;
}

static void
record_module_free(void *module)
{
    record_module_clear((PyObject *)module);
}

static PyModuleDef_Slot record_module_slots[] = {
    {Py_mod_exec, record_module_exec},
#ifdef Py_mod_multiple_interpreters
    {Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED},
#endif
#ifdef Py_mod_gil
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL},
};

static struct PyModuleDef _recordmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "asyncpg.protocol.record",
    .m_size = sizeof(record_module_state),
    .m_methods = record_module_methods,
    .m_slots = record_module_slots,
    .m_traverse = record_module_traverse,
    .m_clear = record_module_clear,
    .m_free = record_module_free,
};

PyMODINIT_FUNC
PyInit_record(void)
{
    return PyModuleDef_Init(&_recordmodule);
}
