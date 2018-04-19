# -*- coding: utf-8 -*-
from __future__ import print_function

from os import chdir
from os.path import abspath, dirname

from setuptools import find_packages, setup

chdir(dirname(abspath(__file__)))

setup(
    name='flask_resource',
    version_command=('git describe --tags --dirty', 'pep440-git-full'),
    description='Presents PynamoDB models as flask-RESTful resources',
    author='Brandon Davidson',
    author_email='brad@oatmail.org',
    packages=find_packages(),
    include_package_data=False,
    install_requires=[
        'flask-restful',
        'pynamodb',
        'ujson',
        'six',
    ],
    extras_require={
        'dev': [
            'setuptools-version-command',
        ]
    },
)
