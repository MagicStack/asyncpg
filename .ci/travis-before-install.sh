#!/bin/bash

if [ "${TRAVIS_OS_NAME}" == "linux" ]; then
    # Allow docker guests to connect to the database
    sudo service postgresql stop 9.4
    echo "listen_addresses = '*'" | \
        sudo tee --append /etc/postgresql/9.4/main/postgresql.conf
    echo "host all all 172.17.0.0/16 trust" | \
        sudo tee --append /etc/postgresql/9.4/main/pg_hba.conf
    sudo service postgresql start 9.4
fi
