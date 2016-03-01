# -*- coding: utf-8 -*-
from os.path import join, dirname
from setuptools import setup, find_packages
import sys
import os

VERSION = (2, 0, 0)
__version__ = VERSION
__versionstr__ = '.'.join(map(str, VERSION))

f = open(join(dirname(__file__), 'es_sql', 'README.md'))
long_description = f.read().strip()
f.close()

install_requires = [
]
tests_require = [
]

# use external unittest for 2.6
if sys.version_info[:2] == (2, 6):
    install_requires.append('unittest2')

setup(
        name = 'es-sql',
        description = "Use sql to query from Elasticsearch",
        license="Apache License, Version 2.0",
        url = "https://github.com/taowen/es-monitor",
        long_description = long_description,
        version = __versionstr__,
        author = "Tao Wen",
        author_email = "taowen@gmail.com",
        packages=find_packages(
                where='.',
                include=('es_sql*', )
        ),
        keywords="sql elasticsearch es",
        classifiers = [
            "Development Status :: 4 - Beta",
            "License :: OSI Approved :: Apache Software License",
            "Intended Audience :: Developers",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 2.6",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: Implementation :: CPython",
            "Programming Language :: Python :: Implementation :: PyPy",
        ],
        install_requires=install_requires,
        entry_points={
            'console_scripts': [
                'es-sql = es_sql.__main__:main'
            ]
        },
        test_suite='es_sql.run_tests.run_all',
        tests_require=tests_require,
)