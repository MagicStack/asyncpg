#!/bin/bash

set -Eexuo pipefail

if [ "$RUNNER_OS" == "Linux" ]; then
    # Assume Ubuntu since this is the only Linux used in CI.
    sudo apt-get update
    sudo apt-get install -y --no-install-recommends \
        libkrb5-dev krb5-user krb5-kdc krb5-admin-server
fi
