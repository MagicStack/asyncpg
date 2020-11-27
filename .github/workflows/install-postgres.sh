#!/bin/bash

set -Eexuo pipefail
shopt -s nullglob

export DEBIAN_FRONTEND=noninteractive

apt-get install -y --no-install-recommends curl
curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
mkdir -p /etc/apt/sources.list.d/
echo "deb https://apt.postgresql.org/pub/repos/apt/ ${DISTRO_NAME}-pgdg main" \
    >> /etc/apt/sources.list.d/pgdg.list
apt-get update
apt-get install -y --no-install-recommends \
    postgresql-${PGVERSION} \
    postgresql-contrib-${PGVERSION}
