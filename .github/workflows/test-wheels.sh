#!/bin/bash

set -Eexuo pipefail
shopt -s nullglob

pip install --pre -f "file:///${GITHUB_WORKSPACE}/dist" asyncpg
make -C "${GITHUB_WORKSPACE}" testinstalled
