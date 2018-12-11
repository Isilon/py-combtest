#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

try:
    from version import __version__
except ImportError:
    os.system('cp -f ./combtest/version.py .')
    from version import __version__

from setuptools import setup, Command


class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')
        os.system('rm -vrf combtest/version.py')

requirements = []
if os.path.exists("requirements.txt"):
    static_list = []
    with open("requirements.txt") as req_file:
        static_list = req_file.readlines()
        requirements = [x.strip("\n") for x in static_list]

    with open("combtest/data/easy_install_requirements", "w") as static_reqs:
        static_reqs.writelines(static_list)
else:
    with open("./combtest/data/easy_install_requirements") as static_reqs:
        requirements = [x.strip("\n") for x in static_reqs.readlines()]

os.system('cp -f version.py combtest/')

# XXX file in with nose or whatever if needed
test_requirements = [
]

setup(
    name='py-combtest',
    version=__version__,
    description="Combinatorial test case generation and running",
    author="Dell/EMC",
    package_data={"combtest": ["data/easy_install_requirements"]},
    author_email='matthew.bryan@isilon.com',
    url='https://github.west.com/mbryan/py-combtest',
    packages=[
        'py-combtest',
    ],
    install_requires=requirements,
    keywords='combtest',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
    ],
    cmdclass={
        'clean': CleanCommand,
    },
)
