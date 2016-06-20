.PHONY: compile debug test


compile:
	cython asyncpg/protocol.pyx
	python setup.py build_ext --inplace

debug:
	cython -a asyncpg/protocol.pyx
	python setup.py build_ext --inplace --debug

test:
	PYTHONASYNCIODEBUG=1 python -m unittest discover -s tests
	python -m unittest discover -s tests
