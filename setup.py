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

requirements = ['paramiko=2.4',
                'rpyc==3.3',
                'sphinx==1.4']

os.system('cp -f version.py combtest/')

# Holding place for later, if needed.
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
        'combtest',
        'combtest.test.',
        'combtest.test.classes',
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
