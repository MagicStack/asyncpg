#!/bin/bash

set -e -x


if [[ "${TRAVIS_BRANCH}" != "release" || "${BUILD}" != *wheels* ]]; then
    # Not a release
    exit 0
fi


if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    PYENV_ROOT="$HOME/.pyenv"
    PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"
fi

PACKAGE_VERSION=$(python ".ci/package-version.py")
PYPI_VERSION=$(python ".ci/pypi-check.py" "${PYMODULE}")

if [ "${PACKAGE_VERSION}" == "${PYPI_VERSION}" ]; then
    echo "${PYMODULE}-${PACKAGE_VERSION} is already published on PyPI"
    exit 1
fi


_root="${TRAVIS_BUILD_DIR}"


_upload_wheels() {
    python "${_root}/.ci/s3-upload.py" "${_root}/dist"/*.whl
    sudo rm -rf "${_root}/dist"/*.whl
}


pip install -U -r ".ci/requirements-publish.txt"


if [ "${TRAVIS_OS_NAME}" == "linux" ]; then
    for pyver in ${RELEASE_PYTHON_VERSIONS}; do
        ML_PYTHON_VERSION=$(python3 -c \
            "print('cp{maj}{min}-cp{maj}{min}{s}'.format( \
                   maj='${pyver}'.split('.')[0], \
                   min='${pyver}'.split('.')[1],
                   s='m' if tuple('${pyver}'.split('.')) < ('3', '8') \
                     else ''))")

        if [[ "$(uname -m)" = "x86_64" ]]; then
            ARCHES="x86_64 i686"
            MANYLINUX_VERSION="1"
        elif [[ "$(uname -m)" = "aarch64" ]]; then
            ARCHES="aarch64"
            MANYLINUX_VERSION="2014"
        fi

        for arch in $ARCHES; do
            ML_IMAGE="quay.io/pypa/manylinux${MANYLINUX_VERSION}_${arch}"
            docker pull "${ML_IMAGE}"
            docker run --rm \
                -v "${_root}":/io \
                -e "PYMODULE=${PYMODULE}" \
                -e "PYTHON_VERSION=${ML_PYTHON_VERSION}" \
                -e "ASYNCPG_VERSION=${PACKAGE_VERSION}" \
                "${ML_IMAGE}" /io/.ci/build-manylinux-wheels.sh

            _upload_wheels
        done
    done

elif [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    export PGINSTALLATION="/usr/local/opt/postgresql@${PGVERSION}/bin"

    make clean
    python setup.py bdist_wheel --dist-dir /tmp/

    pip install /tmp/*.whl
    mkdir -p "${_root}/dist"
    mv /tmp/*.whl "${_root}/dist/"
    make -C "${_root}" ASYNCPG_VERSION="${PACKAGE_VERSION}" testinstalled

    _upload_wheels

else
    echo "Cannot build on ${TRAVIS_OS_NAME}."
fi
