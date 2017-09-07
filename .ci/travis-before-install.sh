#!/bin/bash

set -e -x

if [ -z "${PGVERSION}" ]; then
    echo "Missing PGVERSION environment variable."
    exit 1
fi

if [[ "${TRAVIS_OS_NAME}" == "linux" && "${BUILD}" == *wheels* ]]; then
    # Allow docker guests to connect to the database
    sudo service postgresql stop ${PGVERSION}
    echo "listen_addresses = '*'" | \
        sudo tee --append /etc/postgresql/${PGVERSION}/main/postgresql.conf
    echo "host all all 172.17.0.0/16 trust" | \
        sudo tee --append /etc/postgresql/${PGVERSION}/main/pg_hba.conf
    sudo service postgresql start ${PGVERSION}
fi

if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    git clone --depth 1 https://github.com/yyuu/pyenv.git ~/.pyenv
    PYENV_ROOT="$HOME/.pyenv"
    PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"

    if ! (pyenv versions | grep "${PYTHON_VERSION}$"); then
        pyenv install ${PYTHON_VERSION}
    fi
    pyenv global ${PYTHON_VERSION}
    pyenv rehash

    # Install PostgreSQL
    brew update >/dev/null
    if brew ls --versions postgresql@${PGVERSION} > /dev/null; then
        brew upgrade postgresql@${PGVERSION}
    else
        brew install postgresql@${PGVERSION}
    fi
fi
