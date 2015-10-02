#!/usr/bin/env python
"""Command-line tool for interacting with PostgreSQL databases."""

from __future__ import unicode_literals

import sys
import logging
from argparse import ArgumentParser

import psycopg2

# Globals
MAINT_DBNAME = 'postgres'  # FIXME: hardcoded
APPNAME = "PGtool"
log = logging.getLogger('pgtool')
PY2 = sys.version_info[0] <= 2
args = None


def connect(database=MAINT_DBNAME, async=False):
    appname = APPNAME
    if args and args.cmd:
        appname += " " + args.cmd
    pg_args = {
        'database': database,
        'application_name': appname,
    }
    if args.host:
        pg_args['host'] = args.host
    if args.port is not None:
        pg_args['port'] = args.port
    if async:
        pg_args['async'] = async

    db = psycopg2.connect(**pg_args)
    # psycopg2 returns only unicode strings
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, db)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, db)
    db.autocommit = True

    return db


def quote_names(db, names):
    """psycopg2 doesn't know how to quote identifier names, so we ask the server"""
    c = db.cursor()
    c.execute("SELECT pg_catalog.quote_ident(n) FROM pg_catalog.unnest(%s::text[]) n", [list(names)])
    return [name for (name,) in c]  # Unpack rows of one column


def terminate(db, databases):
    c = db.cursor()
    c.execute("""\
    SELECT pg_catalog.pg_terminate_backend(pid), application_name, usename, client_addr FROM pg_catalog.pg_stat_activity
        WHERE datname = ANY(%s) AND pid != pg_catalog.pg_backend_pid()
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
    if args.force:
        terminate(db, [args.src, args.dest])

    q_src, q_dest = quote_names(db, (args.src, args.dest))

    c = db.cursor()
    sql = "CREATE DATABASE %s TEMPLATE %s" % (q_dest, q_src)
    log.info("SQL: %s", sql)
    c.execute(sql)

    # Copy database and role settings
    # XXX PostgreSQL 8.4 and older use a different catalog table?
    c.execute("""\
    SELECT r.rolname, pg_catalog.unnest(setconfig)
    FROM pg_catalog.pg_db_role_setting
        JOIN pg_catalog.pg_database d ON (d.oid=setdatabase)
        LEFT JOIN pg_catalog.pg_roles r ON (r.oid=setrole)
    WHERE datname=%s
    """, [args.src])
    for role, setting in c.fetchall():
        key, value = setting.split('=', 1)
        q_role, q_key = quote_names(db, (role, key))

        # See pg_dumpall.c makeAlterConfigCommand: Some GUC variable names are 'LIST' type and hence must not be quoted.
        if key not in ('DateStyle', 'search_path'):
            # noinspection PyArgumentList
            value = psycopg2.extensions.adapt(value).getquoted()

        if role:
            sql = "ALTER ROLE %s IN DATABASE %s SET %s=%s" % (q_role, q_dest, q_key, value)
        else:
            sql = "ALTER DATABASE %s SET %s=%s" % (q_dest, key, value)

        log.info("SQL: %s", sql)
        c.execute(sql)


def pg_move():
    db = connect()
    if args.force:
        terminate(db, [args.src, args.dest])

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


if PY2:
    def unicode_arg(val):
        return val.decode(sys.getfilesystemencoding() or 'utf8')
else:
    unicode_arg = str

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
    parser.add_argument("-f", "--force",
                        action='store_true', dest='force', default=False,
                        help="kill connections automatically if they prevent a command from executing")
    parser.add_argument("--host", metavar="HOST",
                        help="hostname of database server")
    parser.add_argument("-p", "--port", metavar="PORT", type=int,
                        help="port number of database server")
    parser.add_argument('cmd', metavar=cmd or "COMMAND", choices=COMMANDS.keys(),  # XXX Sort of a hack
                        help="select the tool/command")

    if cmd in ('cp', 'mv'):
        parser.add_argument('src', metavar="SOURCE", type=unicode_arg,
                            help="source database name")
        parser.add_argument('dest', metavar="DEST", type=unicode_arg,
                            help="destination database name")

    if cmd == 'kill':
        parser.add_argument('databases', metavar="DBNAME", type=unicode_arg, nargs='+',
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
