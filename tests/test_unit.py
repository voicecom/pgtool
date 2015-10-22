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


def get_single_val(c, sql, vars=None):
    c.execute(sql, vars)
    assert c.rowcount == 1
    return c.fetchone()[0]


def get_rel_oid(c, relname):
    return get_single_val(c, "SELECT %s::regclass::int", [relname])


class OperationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """The environment must contain PG* environment variables to establish a PostgreSQL connection:
        http://www.postgresql.org/docs/current/static/libpq-envars.html
        """
        parser = pgtool.make_argparser()
        pgtool.args = parser.parse_args(['kill', 'x'])  # hack to fill out args
        cls.db = pgtool.connect(None)

        c = cls.db.cursor()
        # XXX ideally this setup should be done once globally, not for each test class
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

    @classmethod
    def tearDownClass(cls):
        # Intentionally don't drop the test schema, so it's easier to inspect failures
        # noinspection PyUnresolvedReferences
        cls.db.close()

    def setUp(self):
        """Reset server-side state of the connection"""
        c = self.db.cursor()
        c.execute("ROLLBACK")
        c.execute("DISCARD ALL")  # cannot be executed from a multi-command string
        c.execute("SET search_path=pgtool_test")

    def internal_test_reindex(self, name, sql):
        c = self.db.cursor()
        # Create index
        c.execute(sql)
        oid1 = get_rel_oid(c, name)
        stmt1 = get_single_val(c, "SELECT pg_get_indexdef(%s, 0, false)", [oid1])

        # Recreate index
        pgtool.pg_reindex(self.db, name)
        oid2 = get_rel_oid(c, name)
        stmt2 = get_single_val(c, "SELECT pg_get_indexdef(%s, 0, false)", [oid2])
        self.assertTrue(oid2 > 0)

        # New oid must be allocated for the new index
        self.assertNotEqual(oid1, oid2)
        # But index expressions must remain equal
        self.assertEqual(stmt1, stmt2)

    def test_reindex_simple(self):
        """Test a simple reindex operation"""
        self.internal_test_reindex('reindex_idx1', "CREATE INDEX reindex_idx1 ON reindex_tbl(txt)")

    def test_reindex_complex(self):
        """Test a complex reindex operation"""
        # Deliberately constructed to confuse the parser of pg_get_indexdef() output
        q_name = '"reindex_idx3 INDEX ""x"" ON Aaä"'
        # Tests recreation with a functional expression, UNIQUE and WHERE predicate
        sql = "CREATE UNIQUE INDEX %s ON reindex_tbl(txt, (txt || 'x')) WHERE txt IS NOT NULL" % q_name
        self.internal_test_reindex(q_name, sql)

    def test_reindex_gist(self):
        """Test reindex of a GiST index"""
        # TODO: Skip on older PostgreSQL versions... self.skipTest()
        self.internal_test_reindex('reindex_idx4',
                                   "CREATE INDEX reindex_idx4 ON reindex_tbl USING gist(('(1,1)'::point))")

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
