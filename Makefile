.PHONY: compile debug test quicktest clean all


PYTHON ?= python
ROOT = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))


all: compile


clean:
	rm -fr dist/ doc/_build/
	rm -fr asyncpg/pgproto/*.c asyncpg/pgproto/*.html
	rm -fr asyncpg/pgproto/codecs/*.html
	rm -fr asyncpg/protocol/*.c asyncpg/protocol/*.html
	rm -fr asyncpg/protocol/*.so build *.egg-info
	rm -fr asyncpg/protocol/codecs/*.html
	find . -name '__pycache__' | xargs rm -rf


compile:
	$(PYTHON) setup.py build_ext --inplace --cython-always


debug:
	ASYNCPG_DEBUG=1 $(PYTHON) setup.py build_ext --inplace


test:
	PYTHONASYNCIODEBUG=1 $(PYTHON) setup.py test
	$(PYTHON) setup.py test
	USE_UVLOOP=1 $(PYTHON) setup.py test


testinstalled:
	cd "$${HOME}" && $(PYTHON) $(ROOT)/tests/__init__.py


quicktest:
	$(PYTHON) setup.py test


htmldocs:
	$(PYTHON) setup.py build_ext --inplace
	$(MAKE) -C docs html
