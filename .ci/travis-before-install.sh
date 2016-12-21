#!/bin/bash

if [ "${TRAVIS_OS_NAME}" == "linux" -a "${BUILD}" == "full" ]; then
    # Allow docker guests to connect to the database
    sudo service postgresql stop ${PGVERSION}
    echo "listen_addresses = '*'" | \
        sudo tee --append /etc/postgresql/${PGVERSION}/main/postgresql.conf
    echo "host all all 172.17.0.0/16 trust" | \
        sudo tee --append /etc/postgresql/${PGVERSION}/main/pg_hba.conf
    sudo service postgresql start ${PGVERSION}
fi
