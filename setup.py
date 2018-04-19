# -*- coding: utf-8 -*-
from __future__ import print_function

from os import chdir
from os.path import abspath, dirname

from setuptools import find_packages, setup

chdir(dirname(abspath(__file__)))

setup(
    name='flask-pynamodb-resource',
    version_command=('git describe --tags --dirty', 'pep440-git-full'),
    description='Presents PynamoDB models (DynamoDB tables) as Flask-RESTful resources',
    author='Brandon Davidson',
    author_email='brad@oatmail.org',
    url='https://github.com/brandond/flask-pynamodb-resource',
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
    license='Apache',
    python_requires='>=2.7',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database :: Front-Ends',
        'Framework :: Flask'
    ],
)
