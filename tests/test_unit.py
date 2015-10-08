# -*- coding: utf-8 -*-
"""Unit tests for internal functions"""

from __future__ import unicode_literals

import time
import unittest

from pgtool import pgtool


class MicroTest(unittest.TestCase):
    def setUp(self):
        """The environment must contain PG* environment variables to establish a PostgreSQL connection:
        http://www.postgresql.org/docs/current/static/libpq-envars.html
        """
        pgtool.args = pgtool.parse_args(['kill', 'x'])  # hack :(
        self.db = pgtool.connect()

    def tearDown(self):
        self.db.close()

    def test_quote_names(self):
        """Tests escaping of PostgreSQL identifiers"""
        testcases = {
            'not_reserved_identifier': 'not_reserved_identifier',
            'select': '"select"',
            'MiXeD': '"MiXeD"',
            '\'"\\': '"\'""\\"',
            'õäöü\u2603': '"õäöü\u2603"',
        }

        # 4.10.1. Dictionary view objects: If keys, values and items views are iterated over with no intervening
        # modifications to the dictionary, the order of items will directly correspond.
        self.assertEqual(pgtool.quote_names(self.db, testcases.keys()), list(testcases.values()))

    def test_db_exists(self):
        """Tests for database existance"""
        self.assertTrue(pgtool.db_exists(self.db, 'template0'))  # This database should be un-droppable
        self.assertFalse(pgtool.db_exists(self.db, 'such a long database name could not possibly exist in PostgreSQL'))

    def test_alt_dbname(self):
        """Test generation of temp/backup database names"""
        self.assertEqual(pgtool.generate_alt_dbname(self.db, 'template0'), time.strftime('template0_tmp_%Y%m%d'))
        self.assertEqual(
            pgtool.generate_alt_dbname(self.db, 'such a long database name could not possibly exist in PostgreSQL'),
            time.strftime('such a long database name could not possibly exist_tmp_%Y%m%d'))


if __name__ == '__main__':
    unittest.main()
