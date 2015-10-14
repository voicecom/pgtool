"""Functional tests using the command line interface"""

from __future__ import unicode_literals

import unittest

from pgtool import pgtool


class ArgumentTest(unittest.TestCase):
    """Test command line argument handling"""

    def test_help(self):
        # Help is also printed using AbortWithHelp when no arguments are supplied.
        with self.assertRaises(SystemExit, msg="1"):
            pgtool.main([])
        # WTF, Python 2.7 argparse handles this and prints error; Python 3 returns subcommand None back to the program.
        # I dunno how to make the behavior consistent.
        with self.assertRaises(SystemExit if pgtool.PY2 else pgtool.AbortWithHelp):
            pgtool.main(['--traceback'])

    def test_parser(self):
        parser = pgtool.make_argparser()

        with self.assertRaises(SystemExit, msg="0"):
            parser.parse_args(['--help'])
        # kill doesn't take --no-backup
        with self.assertRaises(SystemExit, msg="1"):
            parser.parse_args(['kill', 'foo', '--no-backup'])
        # mv does
        parser.parse_args(['mv', 'foo', 'bar', '--no-backup'])
        # but not before the command itself
        with self.assertRaises(SystemExit, msg="1"):
            parser.parse_args(['--no-backup', 'mv', 'foo', 'bar'])

        # --quiet is only accepted before command (because argparse hates users)
        parser.parse_args(['--quiet', 'kill', 'foo'])
        with self.assertRaises(SystemExit, msg="1"):
            parser.parse_args(['kill', '--quiet', 'foo'])


if __name__ == '__main__':
    unittest.main()
