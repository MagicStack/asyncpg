#!/bin/bash

set -e -x

if [ -z "${TRAVIS_TAG}" ]; then
    # Not a tagged commit.
    exit 0
fi

pip install -U -r ".ci/requirements-publish.txt"

PACKAGE_VERSION=$(python ".ci/package-version.py")
PYPI_VERSION=$(python ".ci/pypi-check.py" "${PYMODULE}")

if [ "${PACKAGE_VERSION}" == "${PYPI_VERSION}" ]; then
    echo "${PYMODULE}-${PACKAGE_VERSION} is already published on PyPI"
    exit 0
fi

# Check if all expected wheels have been built and uploaded.
if [[ "$(uname -m)" = "x86_64" ]]; then
    release_platforms=(
        "macosx_10_??_x86_64"
        "manylinux1_i686"
        "manylinux1_x86_64"
        "win32"
        "win_amd64"
    )
elif [[ "$(uname -m)" = "aarch64" ]]; then
    release_platforms="manylinux2014_aarch64"
fi

P="${PYMODULE}-${PACKAGE_VERSION}"
expected_wheels=()

for pyver in ${RELEASE_PYTHON_VERSIONS}; do
    abitag=$(python -c \
        "print('cp{maj}{min}-cp{maj}{min}{s}'.format( \
                maj='${pyver}'.split('.')[0], \
                min='${pyver}'.split('.')[1],
                s='m' if tuple('${pyver}'.split('.')) < ('3', '8') else ''))")
    for plat in "${release_platforms[@]}"; do
        expected_wheels+=("${P}-${abitag}-${plat}.whl")
    done
done

rm -rf dist/*.whl dist/*.tar.*
python setup.py sdist
python ".ci/s3-download-release.py" --destdir=dist/ "${P}"

_file_exists() { [[ -f $1 ]]; }

for distfile in "${expected_wheels[@]}"; do
    if ! _file_exists dist/${distfile}; then
        echo "Expected wheel ${distfile} not found."
        exit 1
    fi
done

python -m twine upload dist/*.whl dist/*.tar.*
