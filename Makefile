.PHONY: compile debug test clean check-env all


PYTHON ?= python


all: compile


clean:
	rm -fr dist/ doc/_build/
	rm -fr asyncpg/protocol/*.c asyncpg/protocol/*.html
	rm -fr asyncpg/protocol/*.so build *.egg-info
	rm -fr asyncpg/protocol/codecs/*.html
	find . -name '__pycache__' | xargs rm -rf


check-env:
	$(PYTHON) -c "import cython; (cython.__version__ < '0.24') and exit(1)"


compile: check-env clean
	echo "DEF DEBUG = 0" > asyncpg/protocol/__debug.pxi
	cython asyncpg/protocol/protocol.pyx; rm asyncpg/protocol/__debug.pxi
	@echo "$$CYTHON_BUILD_PATCH_SCRIPT" | $(PYTHON)
	$(PYTHON) setup.py build_ext --inplace


debug: check-env clean
	echo "DEF DEBUG = 1" > asyncpg/protocol/__debug.pxi
	cython -a asyncpg/protocol/protocol.pyx; rm asyncpg/protocol/__debug.pxi
	@echo "$$CYTHON_BUILD_PATCH_SCRIPT" | $(PYTHON)
	$(PYTHON) setup.py build_ext --inplace --debug


test:
	PYTHONASYNCIODEBUG=1 $(PYTHON) -m unittest discover -s tests
	$(PYTHON) -m unittest discover -s tests
	USE_UVLOOP=1 $(PYTHON) -m unittest discover -s tests


sdist: clean compile test
	$(PYTHON) setup.py sdist


release: clean compile test
	$(PYTHON) setup.py sdist upload


# Script to patch Cython 'async def' coroutines to have a 'tp_iter' slot,
# which makes them compatible with 'yield from' without the
# `asyncio.coroutine` decorator.
define CYTHON_BUILD_PATCH_SCRIPT
import re

with open('asyncpg/protocol/protocol.c', 'rt') as f:
    src = f.read()

src = re.sub(
    r'''
    \s* offsetof\(__pyx_CoroutineObject,\s*gi_weakreflist\),
    \s* 0,
    \s* 0,
    \s* __pyx_Coroutine_methods,
    \s* __pyx_Coroutine_memberlist,
    \s* __pyx_Coroutine_getsets,
    ''',

    r'''
    offsetof(__pyx_CoroutineObject, gi_weakreflist),
    __Pyx_Coroutine_await, /* tp_iter */
    0,
    __pyx_Coroutine_methods,
    __pyx_Coroutine_memberlist,
    __pyx_Coroutine_getsets,
    ''',

    src, flags=re.X)

# Fix a segfault in Cython.
src = re.sub(
    r'''
    \s* __Pyx_Coroutine_get_qualname\(__pyx_CoroutineObject\s+\*self\)
    \s* {
    \s* Py_INCREF\(self->gi_qualname\);
    ''',

    r'''
    __Pyx_Coroutine_get_qualname(__pyx_CoroutineObject *self)
    {
        if (self->gi_qualname == NULL) { return __pyx_empty_unicode; }
        Py_INCREF(self->gi_qualname);
    ''',

    src, flags=re.X)

src = re.sub(
    r'''
    \s* __Pyx_Coroutine_get_name\(__pyx_CoroutineObject\s+\*self\)
    \s* {
    \s* Py_INCREF\(self->gi_name\);
    ''',

    r'''
    __Pyx_Coroutine_get_name(__pyx_CoroutineObject *self)
    {
        if (self->gi_name == NULL) { return __pyx_empty_unicode; }
        Py_INCREF(self->gi_name);
    ''',

    src, flags=re.X)

with open('asyncpg/protocol/protocol.c', 'wt') as f:
    f.write(src)
endef
export CYTHON_BUILD_PATCH_SCRIPT
