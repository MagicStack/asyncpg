#!/bin/bash

set -e -x

if [[ "${BUILD}" != *docs* ]]; then
    echo "Skipping documentation build."
    exit 0
fi

pip install -U .[dev]
make htmldocs SPHINXOPTS="-q -W -j4"
