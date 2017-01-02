#!/bin/bash

set -e -x

if [[ "${BUILD}" != *tests* ]]; then
    echo "Skipping tests."
    exit 0
fi

if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    PYENV_ROOT="$HOME/.pyenv"
    PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"
fi

if [[ "${BUILD}" == *quicktests* ]]; then
    make && make quicktest
else
    make && make test
    make clean && make debug && make test
fi
