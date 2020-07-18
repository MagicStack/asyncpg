#!/bin/bash

set -e -x

# iproute isn't included in CentOS 7
yum install -y iproute

# Compile wheels
PYTHON="/opt/python/${PYTHON_VERSION}/bin/python"
PIP="/opt/python/${PYTHON_VERSION}/bin/pip"
${PIP} install --upgrade setuptools pip wheel
cd /io
make clean
${PYTHON} setup.py bdist_wheel

# Bundle external shared libraries into the wheels.
for whl in /io/dist/*.whl; do
    auditwheel repair $whl -w /tmp/
    ${PIP} install /tmp/*.whl
    mv /tmp/*.whl /io/dist/
    rm /io/dist/*-linux_*.whl
done

# Grab docker host, where Postgres should be running.
export PGHOST=$(ip route | awk '/default/ { print $3 }' | uniq)
export PGUSER="postgres"

rm -rf /io/tests/__pycache__
make -C /io PYTHON="${PYTHON}" testinstalled
rm -rf /io/tests/__pycache__
