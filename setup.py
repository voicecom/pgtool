#!/usr/bin/env python

from setuptools import setup

setup(
    name='pgtool',
    version='0.0.1',

    # PyPI metadata
    author='Marti Raudsepp',
    author_email='marti@voicecom.ee',
    url='https://github.com/voicecom/pgtool',
    download_url='https://pypi.python.org/pypi/pgtool/',
    license='Apache Software License',
    description='Command-line tool to simplify some common maintenance tasks on PostgreSQL databases',
    long_description=open('README.rst').read(),
    platforms='any',
    keywords='postgresql tool maintenance admin reindex concurrently rename move copy duplicate databases voicecom',
    classifiers=[
        # https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Database :: Front-Ends',
        'Topic :: System :: Systems Administration',
    ],

    # Installation settings
    packages=['pgtool'],
    entry_points={'console_scripts': ['pgtool = pgtool.pgtool:main']},
    install_requires=[
        'psycopg2>=2.4.2',
    ],
    test_suite='tests',
)
