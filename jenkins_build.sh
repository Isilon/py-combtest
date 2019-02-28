#!/bin/sh -xe

rm -rf venv build dist *.egg-info

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

mkdir venv
virtualenv venv
. venv/bin/activate
which python
python setup.py clean
pip install -r requirements.txt
python setup.py build
python setup.py install
#cp -r venv/lib/python2.7/site-packages/py-combtest-*.egg/isilon/accountant venv/lib/python2.7/site-packages/isilon
#mkdir task_logs
python setup.py test
deactivate
rm -rf venv
python setup.py clean
python setup.py sdist
