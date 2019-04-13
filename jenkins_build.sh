#!/bin/bash -xe

rm -rf venv build dist *.egg-info

make docker-test
rc=$?;
if [[ $rc != 0 ]]; then
    echo "Test failure; exiting"
    exit $rc;
fi

make docker-dist
