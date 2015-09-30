#!/usr/bin/env python
"""Command-line tool for interacting with PostgreSQL databases."""

from __future__ import unicode_literals

import sys
import logging
from argparse import ArgumentParser

import psycopg2

# Globals
MAINT_DBNAME = 'postgres'  # FIXME: hardcoded
log = logging.getLogger('pgtool')
PY2 = sys.version_info[0] <= 2
args = None


def connect(database=MAINT_DBNAME, async=False):
    pg_args = {
        'database': database
    }
    if args.host:
        pg_args['host'] = args.host
    if args.port is not None:
        pg_args['port'] = args.port
    if async:
        pg_args['async'] = async

    db = psycopg2.connect(**pg_args)
    db.autocommit = True

    return db


def quote_names(db, names):
    c = db.cursor()
    c.execute("SELECT quote_ident(n) FROM unnest(%s) n", [list(names)])
    return (name for (name,) in c)  # Unpack rows of one column


def terminate(db, databases):
    c = db.cursor()
    c.execute("""\
    SELECT pg_terminate_backend(pid), application_name, usename, client_addr FROM pg_stat_activity
        WHERE datname = ANY(%s) AND pid != pg_backend_pid()
    """, [databases])

    count = 0
    for term, app, user, addr in c:
        log.info("%s %s by %s@%s",
                 "Killed" if term else "Cannot kill",
                 app if app else "(unknown)", user, addr)
        if term:
            count += 1

    if count:
        log.warn("Killed %d connection(s)", count)
    return count


def pg_copy():
    db = connect()
    q_src, q_dest = quote_names(db, (args.src, args.dest))

    c = db.cursor()
    sql = "CREATE DATABASE %s TEMPLATE %s" % (q_dest, q_src)
    log.info("SQL: %s", sql)
    c.execute(sql)


def pg_move():
    db = connect()
    q_src, q_dest = quote_names(db, (args.src, args.dest))

    c = db.cursor()
    sql = "ALTER DATABASE %s RENAME TO %s" % (q_src, q_dest)
    log.info("SQL: %s", sql)
    c.execute(sql)


def pg_kill():
    db = connect()
    count = terminate(db, args.databases)
    if count == 0:
        log.error("No connections could be killed")
        # Return status 1, like killall
        sys.exit(1)


COMMANDS = {
    'cp': pg_copy,
    'mv': pg_move,
    'kill': pg_kill,
}


def parse_args(argv=None):
    """Argument parsing. Keep this together with main()"""

    if argv is None:
        argv = sys.argv[1:]

    # XXX Maybe there's a cleaner solution for this?
    cmd = [a for a in argv if not a.startswith(str('-'))]
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
    parser.add_argument("-p", "--port", metavar="PORT", type=int,
                        help="port number of database server")
    parser.add_argument('cmd', metavar=cmd or "COMMAND", choices=COMMANDS.keys(),  # XXX Sort of a hack
                        help="select the tool/command")

    if cmd in ('cp', 'mv'):
        parser.add_argument('src', metavar="SOURCE",
                            help="source database name")
        parser.add_argument('dest', metavar="DEST",
                            help="destination database name")

    if cmd == 'kill':
        parser.add_argument('databases', metavar="DBNAME", nargs='+',
                            help="kill connections on this database")

    return parser.parse_args(argv)


def main(argv=None):
    global args

    args = parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format='%(message)s'
    )

    tool = COMMANDS[args.cmd]
    tool()


if __name__ == '__main__':
    main()
