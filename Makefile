.PHONY: compile debug test


compile:
	cython asyncpg/protocol.pyx
	python setup.py build_ext --inplace

debug:
	cython -a asyncpg/protocol.pyx
	python setup.py build_ext --inplace

test:
	python3 -m unittest discover -s tests -v
