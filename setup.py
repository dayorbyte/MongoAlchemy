#!/usr/bin/env python

from distutils.core import setup

VERSION = '0.13.2'
DESCRIPTION = 'Document-Object Mapper/Toolkit for Mongo Databases'

setup(
    name='MongoAlchemy',
    version=VERSION,
    description=DESCRIPTION,
    author='Jeffrey Jenkins',
    license='MIT',
    author_email='jeff@qcircles.net',
    url='http://mongoalchemy.org/',
    packages=['mongoalchemy'],
    install_requires=['pymongo'],
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)