#!/bin/bash

set -e -x

if [ "${BUILD}" != "quick" -a "${BUILD}" != "full" ]; then
    echo "Skipping tests."
    exit 0
fi

if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    PYENV_ROOT="$HOME/.pyenv"
    PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"
fi

if [ "${BUILD}" == "quick" ]; then
    make && make quicktest
else
    make && make test
    make debug && make test
fi
