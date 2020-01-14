#!/bin/bash

set -e -x

if [ -z "${PGVERSION}" ]; then
    echo "Missing PGVERSION environment variable."
    exit 1
fi

if [[ "${TRAVIS_OS_NAME}" == "linux" && "${BUILD}" == *wheels* ]]; then
    sudo service postgresql stop ${PGVERSION}

    echo "port = 5432" | \
        sudo tee --append /etc/postgresql/${PGVERSION}/main/postgresql.conf

    if [[ "${BUILD}" == *wheels* ]]; then
        # Allow docker guests to connect to the database
        echo "listen_addresses = '*'" | \
            sudo tee --append /etc/postgresql/${PGVERSION}/main/postgresql.conf
        echo "host all all 172.17.0.0/16 trust" | \
            sudo tee --append /etc/postgresql/${PGVERSION}/main/pg_hba.conf

        if [ "${PGVERSION}" -ge "11" ]; then
            # Disable JIT to avoid unpredictable timings in tests.
            echo "jit = off" | \
                sudo tee --append /etc/postgresql/${PGVERSION}/main/postgresql.conf
        fi
    fi

    sudo pg_ctlcluster ${PGVERSION} main restart
fi

if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    brew update >/dev/null
    brew upgrade pyenv
    eval "$(pyenv init -)"

    if ! (pyenv versions | grep "${PYTHON_VERSION}$"); then
        pyenv install ${PYTHON_VERSION}
    fi
    pyenv global ${PYTHON_VERSION}
    pyenv rehash

    # Install PostgreSQL
    if brew ls --versions postgresql > /dev/null; then
        brew remove --force --ignore-dependencies postgresql
    fi

    brew install postgresql@${PGVERSION}
    brew services start postgresql@${PGVERSION}
fi
