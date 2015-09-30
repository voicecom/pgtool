#!/usr/bin/env python

from setuptools import setup

setup(
    name='pgtool',
    version='0.0.0',

    # PyPI metadata
    author='Marti Raudsepp',
    author_email='marti@juffo.org',
    url='https://github.com/intgr/pgtool',
    download_url='https://pypi.python.org/pypi/pgtool/',
    license='MIT',
    description='',  # TODO
    long_description=open('README.rst').read(),
    platforms='any',
    keywords='',  # TODO
    classifiers=[
        # TODO https://pypi.python.org/pypi?%3Aaction=list_classifiers
    ],

    # Installation settings
    packages=['pgtool'],
    entry_points={'console_scripts': ['pgtool = pgtool.pgtool:main']},
    install_requires=[
        'psycopg2>=2.4',
    ],
    test_suite='tests',
)
