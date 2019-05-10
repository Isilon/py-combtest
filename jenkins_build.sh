#!/bin/bash -xe

rm -rf venv build dist *.egg-info

replacement="__build__ = $BUILD_NUMBER"
find . -name version.py | xargs sed -i "s|^__build__ =.*|$replacement|"

make docker-test
rc=$?;
if [[ $rc != 0 ]]; then
    echo "Test failure; exiting"
    exit $rc;
fi

make docker-dist
