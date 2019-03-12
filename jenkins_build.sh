#!/bin/bash -xe

rm -rf venv build dist *.egg-info

if [ ! -z "$BUILD_NUMBER" ]; then
    replacement="__build__ = $BUILD_NUMBER"
    find . -name version.py | xargs sed -i "s|^__build__ =.*|$replacement|"

    repo_host=artifactory.west.isilon.com

    if [ -n "$1" ]; then
       repo_host=$1
    fi

    repo_url=http://$repo_host:8081/artifactory/api/pypi/pypi-repo/simple
    res=`pip install -i "$repo_url" --trusted-host "$repo_host" py-combtest==list-versions 2>&1 | grep $(python -c 'import version; print(version.__version__)')` || true

    if [ -z "$res" ]; then
        echo "Version number is OK"
    else
        echo "Version number is already used. Please bump the version number!!!"
        exit 1
    fi
fi

#make docker-test
#rc=$?;
#if [[ $rc != 0 ]]; then
#    echo "Test failure; exiting"
#    exit $rc;
#fi

echo "My setuptools version is:"
python -c "import setuptools; print setuptools.__version__"

make dist
