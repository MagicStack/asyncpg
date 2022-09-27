#!/bin/bash

set -Eexuo pipefail
shopt -s nullglob

PGVERSION=${PGVERSION:-12}

if [ -e /etc/os-release ]; then
    source /etc/os-release
elif [ -e /etc/centos-release ]; then
    ID="centos"
    VERSION_ID=$(cat /etc/centos-release | cut -f3 -d' ' | cut -f1 -d.)
else
    echo "install-postgres.sh: cannot determine which Linux distro this is" >&2
    exit 1
fi

if [ "${ID}" = "debian" -o "${ID}" = "ubuntu" ]; then
    export DEBIAN_FRONTEND=noninteractive

    apt-get install -y --no-install-recommends curl gnupg ca-certificates
    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
    mkdir -p /etc/apt/sources.list.d/
    echo "deb https://apt.postgresql.org/pub/repos/apt/ ${VERSION_CODENAME}-pgdg main" \
        >> /etc/apt/sources.list.d/pgdg.list
    apt-get update
    apt-get install -y --no-install-recommends \
        "postgresql-${PGVERSION}" \
        "postgresql-contrib-${PGVERSION}"
elif [ "${ID}" = "almalinux" ]; then
    yum install -y \
        "postgresql-server" \
        "postgresql-devel" \
        "postgresql-contrib"
elif [ "${ID}" = "centos" ]; then
    el="EL-${VERSION_ID%.*}-$(arch)"
    baseurl="https://download.postgresql.org/pub/repos/yum/reporpms"
    yum install -y "${baseurl}/${el}/pgdg-redhat-repo-latest.noarch.rpm"
    if [ ${VERSION_ID%.*} -ge 8 ]; then
        dnf -qy module disable postgresql
    fi
    yum install -y \
        "postgresql${PGVERSION}-server" \
        "postgresql${PGVERSION}-contrib"
    ln -s "/usr/pgsql-${PGVERSION}/bin/pg_config" "/usr/local/bin/pg_config"
elif [ "${ID}" = "alpine" ]; then
    apk add shadow postgresql postgresql-dev postgresql-contrib
else
    echo "install-postgres.sh: Unsupported distro: ${distro}" >&2
    exit 1
fi

useradd -m -s /bin/bash apgtest
