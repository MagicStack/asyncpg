#!/bin/bash

set -e -x

if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    brew update

    brew outdated postgresql || brew upgrade postgresql

    brew outdated pyenv || brew upgrade pyenv
    eval "$(pyenv init -)"
    pyenv versions

    if ! (pyenv versions | grep "${PYTHON_VERSION}$"); then
        pyenv install ${PYTHON_VERSION}
    fi
    pyenv local ${PYTHON_VERSION}
fi

pip install --upgrade pip wheel
pip install --upgrade setuptools
pip install -r .ci/requirements.txt
