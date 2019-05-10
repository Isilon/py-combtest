#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

# Provides __version__
exec(open('version.py').read())

from setuptools import setup, find_packages, Command


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

requirements = ['rpyc~=4.0',
                'six~=1.12',
                'sphinx~=1.4']

os.system('cp -f version.py combtest/')

# Holding place for later, if needed.
test_requirements = [
]

setup(
    name='py-combtest',
    version=__version__,
    description="Combinatorial test case generation and running",
    author="Dell/EMC",
    include_package_data=True,
    author_email='matthew.bryan@isilon.com',
    url='https://github.west.com/mbryan/py-combtest',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    python_requires='>=2.7,!=3.0,!=3.1,!=3.2,!=3.3,!=3.4,<4',
    install_requires=requirements,
    keywords='combtest',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    cmdclass={
        'clean': CleanCommand,
    },
)
