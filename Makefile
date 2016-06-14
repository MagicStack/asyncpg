.PHONY: compile test


compile:
	cython asyncpg/protocol.pyx
	python setup.py build_ext --inplace


test:
	python3 -m unittest discover -s tests -v
