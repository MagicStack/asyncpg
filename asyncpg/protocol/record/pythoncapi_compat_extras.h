#ifndef PYTHONCAPI_COMPAT_EXTRAS
#define PYTHONCAPI_COMPAT_EXTRAS

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>

// Python 3.11.0a6 added PyType_GetModuleByDef() to Python.h
#if PY_VERSION_HEX < 0x030b00A6
PyObject *
PyType_GetModuleByDef(PyTypeObject *type, PyModuleDef *def)
{
    assert(PyType_Check(type));

    if (!PyType_HasFeature(type, Py_TPFLAGS_HEAPTYPE)) {
        // type_ready_mro() ensures that no heap type is
        // contained in a static type MRO.
        goto error;
    }
    else {
        PyHeapTypeObject *ht = (PyHeapTypeObject*)type;
        PyObject *module = ht->ht_module;
        if (module && PyModule_GetDef(module) == def) {
            return module;
        }
    }

    PyObject *res = NULL;
    PyObject *mro = type->tp_mro;
    // The type must be ready
    assert(mro != NULL);
    assert(PyTuple_Check(mro));
    // mro_invoke() ensures that the type MRO cannot be empty.
    assert(PyTuple_GET_SIZE(mro) >= 1);
    // Also, the first item in the MRO is the type itself, which
    // we already checked above. We skip it in the loop.
    assert(PyTuple_GET_ITEM(mro, 0) == (PyObject *)type);

    Py_ssize_t n = PyTuple_GET_SIZE(mro);
    for (Py_ssize_t i = 1; i < n; i++) {
        PyObject *super = PyTuple_GET_ITEM(mro, i);
        if (!PyType_HasFeature((PyTypeObject *)super, Py_TPFLAGS_HEAPTYPE)) {
            // Static types in the MRO need to be skipped
            continue;
        }

        PyHeapTypeObject *ht = (PyHeapTypeObject*)super;
        PyObject *module = ht->ht_module;
        if (module && PyModule_GetDef(module) == def) {
            res = module;
            break;
        }
    }

    if (res != NULL) {
        return res;
    }
error:
    PyErr_Format(
        PyExc_TypeError,
        "PyType_GetModuleByDef: No superclass of '%s' has the given module",
        type->tp_name);
    return NULL;
}
#endif

#ifdef __cplusplus
}
#endif
#endif  // PYTHONCAPI_COMPAT_EXTRAS
