.PHONY: compile debug test quicktest clean all


PYTHON ?= python
ROOT = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))


all: compile


clean:
	rm -fr dist/ doc/_build/
	rm -fr asyncpg/pgproto/*.c asyncpg/pgproto/*.html
	rm -fr asyncpg/pgproto/codecs/*.html
	rm -fr asyncpg/pgproto/*.so
	rm -fr asyncpg/protocol/*.c asyncpg/protocol/*.html
	rm -fr asyncpg/protocol/*.so build *.egg-info
	rm -fr asyncpg/protocol/codecs/*.html
	find . -name '__pycache__' | xargs rm -rf


compile:
	env ASYNCPG_BUILD_CYTHON_ALWAYS=1 $(PYTHON) -m pip install -e .


debug:
	env ASYNCPG_DEBUG=1 $(PYTHON) -m pip install -e .

test:
	PYTHONASYNCIODEBUG=1 $(PYTHON) -m unittest -v tests.suite
	$(PYTHON) -m unittest -v tests.suite
	USE_UVLOOP=1 $(PYTHON) -m unittest -v tests.suite


testinstalled:
	cd "$${HOME}" && $(PYTHON) $(ROOT)/tests/__init__.py


quicktest:
	$(PYTHON) -m unittest -v tests.suite


htmldocs:
	$(PYTHON) -m pip install -e .[docs]
	$(MAKE) -C docs html
