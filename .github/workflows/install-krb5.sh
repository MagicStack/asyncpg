#!/bin/bash

set -Eexuo pipefail
shopt -s nullglob

if [[ $OSTYPE == linux* ]]; then
    if [ "$(id -u)" = "0" ]; then
        SUDO=
    else
        SUDO=sudo
    fi

    if [ -e /etc/os-release ]; then
        source /etc/os-release
    elif [ -e /etc/centos-release ]; then
        ID="centos"
        VERSION_ID=$(cat /etc/centos-release | cut -f3 -d' ' | cut -f1 -d.)
    else
        echo "install-krb5.sh: cannot determine which Linux distro this is" >&2
        exit 1
    fi

    if [ "${ID}" = "debian" -o "${ID}" = "ubuntu" ]; then
        export DEBIAN_FRONTEND=noninteractive

        $SUDO apt-get update
        $SUDO apt-get install -y --no-install-recommends \
            libkrb5-dev krb5-user krb5-kdc krb5-admin-server
    elif [ "${ID}" = "almalinux" ]; then
        $SUDO dnf install -y krb5-server krb5-workstation krb5-libs krb5-devel
    elif [ "${ID}" = "centos" ]; then
        $SUDO yum install -y krb5-server krb5-workstation krb5-libs krb5-devel
    elif [ "${ID}" = "alpine" ]; then
        $SUDO apk add krb5 krb5-server krb5-dev
    else
        echo "install-krb5.sh: Unsupported linux distro: ${distro}" >&2
        exit 1
    fi
else
    echo "install-krb5.sh: unsupported OS: ${OSTYPE}" >&2
    exit 1
fi
