#!/usr/bin/env python
"""Command-line tool for interacting with PostgreSQL databases."""

from __future__ import unicode_literals
import sys
import logging
from argparse import ArgumentParser


log = logging.getLogger('pgtool')
PY2 = sys.version_info[0] <= 2


def pg_copy(args):
    print args


COMMANDS = {
    'cp': pg_copy,
    # 'mv': pg_move,
    # 'kill': pg_kill,
}


def parse_args(argv=None):
    """Argument parsing. Keep this together with main()"""

    if argv is None:
        argv = sys.argv[1:]

    # XXX Maybe there's a cleaner solution for this?
    cmd = [a for a in argv if not a.startswith('-')]
    if len(cmd) >= 1:
        cmd = cmd[0]
    else:
        cmd = None

    # Generic options
    parser = ArgumentParser()
    parser.add_argument("-q", "--quiet",
                        action='store_true', dest='quiet', default=False,
                        help="silence information messages")
    parser.add_argument("--host", metavar="HOST",
                        help="hostname of database server")
    parser.add_argument('cmd', metavar=cmd or "COMMAND", choices=COMMANDS.keys(),  # XXX Sort of a hack
                        help="select the tool/command")

    if cmd == 'cp':
        parser.add_argument('source', metavar="SRC",
                            help="source database name")
        parser.add_argument('dest', metavar="DEST",
                            help="destination database name")

    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format='%(message)s'
    )

    tool = COMMANDS[args.cmd]
    tool(args)


if __name__ == '__main__':
    main()
