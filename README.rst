PGtool
======
.. image:: https://badge.fury.io/py/pgtool.svg
   :target: http://badge.fury.io/py/pgtool

.. image:: https://travis-ci.org/voicecom/pgtool.svg?branch=master
   :alt: Travis CI
   :target: http://travis-ci.org/voicecom/pgtool

PGtool is a command-line tool designed to simplify some common maintenance tasks on PostgreSQL databases. It works with
Python 2.7 and 3.3+ using the psycopg2 driver.

The easiest way to install it is using pip::

    pip install pgtool

Available commands:

cp SOURCE DEST
    Uses CREATE DATABASE ... TEMPLATE to create a duplicate of a database. Additionally copies over database-specific
    settings.

    When used with --force, an existing database with the same name as DEST is replaced, the original is renamed out of
    place in the form DEST_old_YYYYMMDD (unless --no-backup is specified).

mv SOURCE DEST
    Rename a database within a server.

    When used with --force, an existing database with the same name as DEST is replaced, the original is renamed out of
    place in the form DEST_old_YYYYMMDD (unless --no-backup is specified).

kill DBNAME [DBNAME ...]
    Kills all active connections to the specified database(s).

reindex IDXNAME [IDXNAME ...]
    Uses CREATE INDEX CONCURRENTLY to create a duplicate index, then tries to swap the new index for the original.

    The index swap is done using a short lock timeout to prevent it from interfering with running queries. Retries until
    the rename succeeds.

Resources
---------

* https://github.com/voicecom/pgtool
* https://pypi.python.org/pypi/pgtool

Changelog
---------

0.0.1 (2015-10-26)

* Initial public release, commands: 'cp', 'mv', 'kill' & 'reindex'

Contributing
------------

Code style:

* In general follow the Python PEP-8_ coding style, except line length can go up to 120 chars.
* Strings that have meaning for humans use double quotes (``"``), otherwise single quotes (``'``). When in doubt, don't
  worry about it.
* Code should be compatible with both Python 2 and 3, preferably without version-specific conditionals.

Run the test suite using ``python setup.py test``.

Submit your changes as pull requests on GitHub.

.. _PEP-8: https://www.python.org/dev/peps/pep-0008/

License
-------

Copyright 2015 Voicecom, Marti Raudsepp & contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
