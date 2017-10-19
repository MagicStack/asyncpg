.PHONY: compile debug test quicktest clean all


PYTHON ?= python


all: compile


clean:
	rm -fr dist/ doc/_build/
	rm -fr asyncpg/protocol/*.c asyncpg/protocol/*.html
	rm -fr asyncpg/protocol/*.so build *.egg-info
	rm -fr asyncpg/protocol/codecs/*.html
	find . -name '__pycache__' | xargs rm -rf


compile:
	$(PYTHON) setup.py build_ext --inplace --cython-always


debug:
	$(PYTHON) setup.py build_ext --inplace --debug \
		--cython-always \
		--cython-annotate \
		--cython-directives="linetrace=True" \
		--define ASYNCPG_DEBUG,CYTHON_TRACE,CYTHON_TRACE_NOGIL


test:
	PYTHONASYNCIODEBUG=1 $(PYTHON) setup.py test
	$(PYTHON) setup.py test
	USE_UVLOOP=1 $(PYTHON) setup.py test


testinstalled:
	$(PYTHON) tests/__init__.py
	USE_UVLOOP=1 $(PYTHON) tests/__init__.py


quicktest:
	$(PYTHON) setup.py test


htmldocs: compile
	$(MAKE) -C docs html
