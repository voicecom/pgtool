# -*- coding: utf-8 -*-
"""Unit tests for internal functions"""

from __future__ import unicode_literals

import time
import unittest

import psycopg2

from pgtool import pgtool


class MicroTest(unittest.TestCase):
    def setUp(self):
        """The environment must contain PG* environment variables to establish a PostgreSQL connection:
        http://www.postgresql.org/docs/current/static/libpq-envars.html
        """
        parser = pgtool.make_argparser()
        pgtool.args = parser.parse_args(['kill', 'x'])  # hack :(
        self.db = pgtool.connect(None)

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


def get_rel_oid(c, relname):
    c.execute("SELECT %s::regclass::int", [relname])
    return c.fetchone()[0]


class OperationTest(unittest.TestCase):
    def setUp(self):
        """The environment must contain PG* environment variables to establish a PostgreSQL connection:
        http://www.postgresql.org/docs/current/static/libpq-envars.html
        """
        parser = pgtool.make_argparser()
        pgtool.args = parser.parse_args(['kill', 'x'])  # hack to fill out args
        self.db = pgtool.connect(None)

        c = self.db.cursor()
        # language=SQL
        c.execute("""\
        -- Schema
        DROP SCHEMA IF EXISTS pgtool_test CASCADE;
        CREATE SCHEMA pgtool_test;
        SET search_path=pgtool_test;

        -- Reindex test
        CREATE TABLE reindex_tbl (txt text);
        INSERT INTO reindex_tbl VALUES ('a');
        """)

    def tearDown(self):
        # Intentionally don't drop the test schema, so it's easier to inspect failures
        self.db.close()

    def test_reindex(self):
        """Test a simple reindex operation"""
        c = self.db.cursor()
        # Create index
        c.execute("CREATE INDEX reindex_idx1 ON reindex_tbl(txt)")
        oid1 = get_rel_oid(c, 'reindex_idx1')

        # Recreate index
        pgtool.pg_reindex(self.db, 'reindex_idx1')
        oid2 = get_rel_oid(c, 'reindex_idx1')
        self.assertTrue(oid2 > 0)

        # New oid must be allocated for the new index
        self.assertNotEqual(oid1, oid2)

    def test_reindex_recovery(self):
        """Test error recovery when reindex fails"""
        c = self.db.cursor()
        # Create invalid index. DataError: invalid input syntax for integer: "a"
        with self.assertRaises(psycopg2.DataError):
            c.execute("CREATE INDEX CONCURRENTLY reindex_idx2 ON reindex_tbl((txt::int))")
        oid1 = get_rel_oid(c, 'reindex_idx2')

        # Reindex fails too
        with self.assertRaises(psycopg2.DataError):
            pgtool.pg_reindex(self.db, 'reindex_idx2')
        oid2 = get_rel_oid(c, 'reindex_idx2')

        # Make sure the index wasn't replaced or dropped
        self.assertEqual(oid1, oid2)

        # Make sure we didn't leave behind an invalid index
        c.execute("""\
        SELECT array_agg(indexrelid::regclass::text) FROM pg_catalog.pg_index
            WHERE indrelid='pgtool_test.reindex_tbl'::regclass AND NOT indisvalid
        """)
        self.assertEqual(c.fetchone()[0], ['reindex_idx2'])


if __name__ == '__main__':
    unittest.main()
