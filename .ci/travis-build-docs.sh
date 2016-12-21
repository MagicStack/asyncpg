#!/bin/bash

# Based on https://gist.github.com/domenic/ec8b0fc8ab45f39403dd

set -e -x

if [ "${BUILD}" != "docs" ]; then
    echo "Skipping documentation build."
    exit 0
fi

SOURCE_BRANCH="master"
TARGET_BRANCH="gh-pages"
DOC_BUILD_DIR="_build/html/"

if [ "${TRAVIS_OS_NAME}" != "linux" ]; then
    echo "Skipping documentation build on non-linux."
    exit 0
fi

pip install -r docs/requirements.txt
make htmldocs

if [ "${TRAVIS_PULL_REQUEST}" != "false" \
        -o "${TRAVIS_OS_NAME}" != "linux" ]; then
    echo "Skipping documentation deploy."
    exit 0
fi

if [ "${TRAVIS_BRANCH}" != "${SOURCE_BRANCH}" -a -z "${TRAVIS_TAG}" ]; then
    echo "Skipping documentation deploy."
    exit 0
fi

git config --global user.email "infra@magic.io"
git config --global user.name "Travis CI"

REPO=$(git config remote.origin.url)
SSH_REPO=${REPO/https:\/\/github.com\//git@github.com:}
COMMITISH=$(git rev-parse --verify HEAD)
AUTHOR=$(git show --format="%aN <%aE>" "${COMMITISH}")

git clone "${REPO}" docs/gh-pages
cd docs/gh-pages
git checkout "${TARGET_BRANCH}" || git checkout --orphan "${TARGET_BRANCH}"
cd ..

rm -r gh-pages/devel/
rsync -a "${DOC_BUILD_DIR}/" gh-pages/devel/

if [ -n "${TRAVIS_TAG}" ]; then
    rm -r gh-pages/current/
    rsync -a "${DOC_BUILD_DIR}/" gh-pages/current/
fi

cd gh-pages

if git diff --quiet --exit-code; then
    echo "No changes to documentation."
    exit 0
fi

git add --all .
git commit -m "Automatic documentation update" --author="${AUTHOR}"

set +x
echo "Decrypting push key..."
ENCRYPTED_KEY_VAR="encrypted_${DOCS_PUSH_KEY_LABEL}_key"
ENCRYPTED_IV_VAR="encrypted_${DOCS_PUSH_KEY_LABEL}_iv"
ENCRYPTED_KEY=${!ENCRYPTED_KEY_VAR}
ENCRYPTED_IV=${!ENCRYPTED_IV_VAR}
openssl aes-256-cbc -K "${ENCRYPTED_KEY}" -iv "${ENCRYPTED_IV}" \
            -in "${TRAVIS_BUILD_DIR}/.ci/push_key.enc" \
            -out "${TRAVIS_BUILD_DIR}/.ci/push_key" -d
set -x
chmod 600 "${TRAVIS_BUILD_DIR}/.ci/push_key"
eval `ssh-agent -s`
ssh-add "${TRAVIS_BUILD_DIR}/.ci/push_key"

git push "${SSH_REPO}" "${TARGET_BRANCH}"
rm "${TRAVIS_BUILD_DIR}/.ci/push_key"

cd "${TRAVIS_BUILD_DIR}"
rm -rf docs/gh-pages
