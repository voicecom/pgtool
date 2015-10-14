#!/usr/bin/env python
"""Command-line tool for interacting with PostgreSQL databases."""

from __future__ import unicode_literals

import logging
import re
import sys
from argparse import ArgumentParser

import psycopg2
import psycopg2.errorcodes

# Globals
import time

MAINT_DBNAME = 'postgres'  # FIXME: hardcoded
APPNAME = "PGtool"
MAX_IDENTIFIER_LEN = 63  # http://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
log = logging.getLogger('pgtool')
PY2 = sys.version_info[0] <= 2
args = None


# Utilities
class Abort(Exception):
    """Class for fatal errors reported to the user."""
    # TODO: Proper error handling/reporting
    pass


class AbortWithHelp(Abort):
    """Errors that cause --help screen to be printed."""
    pass


def connect(database=MAINT_DBNAME, async=False):
    appname = APPNAME
    if args and args.cmd:
        appname += " " + args.cmd
    pg_args = {
        'application_name': appname,
    }
    if database:
        pg_args['database'] = database
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
                 app if app else "(unknown)", user,
                 addr if addr else "[local]")
        if term:
            count += 1

    if count:
        log.warn("Killed %d connection(s)", count)
    return count


def db_exists(db, dbname):
    c = db.cursor()
    c.execute("SELECT TRUE FROM pg_catalog.pg_database WHERE datname=%s", [dbname])
    return c.rowcount > 0


def generate_alt_dbname(db, basename, alt='tmp'):
    fail = []
    # Try 5 times...
    for nr in ('', '_1', '_2', '_3', '_4'):
        extension = '_%s_%s%s' % (alt, time.strftime('%Y%m%d'), nr)
        # Truncate basename if necessary to fit into PostgreSQL's 63-byte limit
        # XXX doesn't truncate unicode names properly
        name = basename[:MAX_IDENTIFIER_LEN - len(extension)] + extension
        assert len(name) <= MAX_IDENTIFIER_LEN

        if not db_exists(db, name):
            return name
        fail.append(name)

    raise Abort("Cannot generate unique database name; tried: %s" % ", ".join(fail))


def pg_copy(db, src, dest):
    if args.force:
        terminate(db, [src, dest])

    q_src, q_dest = quote_names(db, (src, dest))

    c = db.cursor()
    sql = "CREATE DATABASE %s TEMPLATE %s" % (q_dest, q_src)
    log.info("SQL: %s", sql)
    try:
        c.execute(sql)
    # BaseException also includes KeyboardInterrupt, Exception doesn't
    except BaseException as err:
        # Just in case, so we don't drop someone else's database
        if getattr(err, 'pgcode', None) not in (psycopg2.errorcodes.DUPLICATE_DATABASE,
                                                psycopg2.errorcodes.UNIQUE_VIOLATION):
            sql = "DROP DATABASE IF EXISTS %s" % q_dest
            log.info("SQL: %s", sql)
            # noinspection PyBroadException
            try:
                c.execute(sql)
            except Exception as err:
                log.error("Error executing DROP: %s", err)
        raise

    # Copy database and role settings
    # XXX PostgreSQL 8.4 and older use a different catalog table?
    c.execute("""\
    SELECT r.rolname, pg_catalog.unnest(setconfig)
    FROM pg_catalog.pg_db_role_setting
        JOIN pg_catalog.pg_database d ON (d.oid=setdatabase)
        LEFT JOIN pg_catalog.pg_roles r ON (r.oid=setrole)
    WHERE datname=%s
    """, [src])
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


def pg_move(db, src, dest):
    if args.force:
        terminate(db, [src, dest])

    q_src, q_dest = quote_names(db, (src, dest))

    c = db.cursor()
    sql = "ALTER DATABASE %s RENAME TO %s" % (q_src, q_dest)
    log.info("SQL: %s", sql)
    c.execute(sql)


def pg_drop(db, name):
    if args.force:
        terminate(db, [name])

    c = db.cursor()
    q_name = quote_names(db, (name,))[0]
    sql = "DROP DATABASE IF EXISTS %s" % q_name
    log.info("SQL: %s", sql)
    c.execute(sql)


def pg_move_extended(db, src, dest):
    if args.force and db_exists(db, dest):
        if args.no_backup:
            pg_drop(db, dest)
        else:
            backup_db = generate_alt_dbname(db, dest, 'old')
            pg_move(db, dest, backup_db)

    pg_move(db, src, dest)


pg_indexdef_re = r"^(CREATE.+) ([^ ]+|\".+\") ON (.+)$"


def pg_reindex(db, idx):
    # This is some hairy code still, but it works :)
    c = db.cursor()

    # XXX regclass case folding is inconsistent with other PGtool commands, but we can live with it for now.
    c.execute("""\
    SELECT nspname, relname, pg_catalog.pg_get_indexdef(c.oid, 0, true)
    FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace ns ON (c.relnamespace=ns.oid)
    WHERE c.oid=%s::pg_catalog.regclass
    """, [idx])
    assert c.rowcount == 1  # Otherwise count==0, regclass should fail anyway
    schema, name, stmt = c.fetchone()

    # TODO Generate unique tmp name or drop previous one when safe?
    tmpname = 'tmp_' + name
    q_schema, q_name, q_tmpname = quote_names(db, [schema, name, tmpname])

    # FIXME Parsing not entirely SQL injection-safe.
    match = re.match(pg_indexdef_re, stmt)
    assert match, "Cannot parse indexdef statement: %s" % stmt

    # TODO Error recovery, drop index if swapping fails.
    sql = "%s CONCURRENTLY %s ON %s" % (match.group(1), q_tmpname, match.group(3))
    log.info("SQL: %s", sql)
    c.execute(sql)

    log.info("Temp index created, trying to swap without interrupting other queries...")
    c.execute("SET lock_timeout='1s'")
    # Retry loop. XXX This may never complete on very busy systems?
    while True:
        try:
            c.execute("BEGIN")
            sql = "DROP INDEX %s.%s" % (q_schema, q_name)
            log.info("SQL: %s", sql)
            c.execute(sql)

            sql = "ALTER INDEX %s.%s RENAME TO %s" % (q_schema, q_tmpname, q_name)
            log.info("SQL: %s", sql)
            c.execute(sql)

        except psycopg2.DatabaseError as err:
            if err.pgcode == psycopg2.errorcodes.LOCK_NOT_AVAILABLE:
                c.execute("ROLLBACK")
                time.sleep(1)
                continue
            raise

        c.execute("COMMIT")  # XXX Can't use db.commit(), why?
        break


def cmd_copy():
    """Uses CREATE DATABASE ... TEMPLATE to create a duplicate of a database. Also copies over database-specific
    settings.

    When used with --force, an existing database with the same name as DEST is replaced, the original is renamed out of
    place in the form DEST_old_YYYYMMDD (unless --no-backup is specified).
    """
    db = connect()

    if args.force and db_exists(db, args.dest):
        tmp_db = generate_alt_dbname(db, args.dest, 'tmp')
        pg_copy(db, args.src, tmp_db)

        pg_move_extended(db, tmp_db, args.dest)

    else:
        pg_copy(db, args.src, args.dest)


def cmd_move(db=None):
    """Rename a database within a server.

    When used with --force, an existing database with the same name as DEST is replaced, the original is renamed out of
    place in the form DEST_old_YYYYMMDD (unless --no-backup is specified).
    """
    if db is None:
        db = connect()

    pg_move_extended(db, args.src, args.dest)


def cmd_kill():
    """Kills all active connections to the specified database(s)."""
    db = connect()
    count = terminate(db, args.databases)
    if count == 0:
        log.error("No connections could be killed")
        # Return status 1, like killall
        sys.exit(1)


def cmd_reindex():
    """Uses CREATE INDEX CONCURRENTLY to create a duplicate index, then tries to swap the new index for the original.

    The index swap is done using a short lock timeout to prevent it from interfering with running queries. Retries until
    the rename succeeds.
    """
    db = connect(args.database)
    for idx in args.indexes:
        pg_reindex(db, idx)


COMMANDS = {
    'cp': cmd_copy,
    'mv': cmd_move,
    'kill': cmd_kill,
    'reindex': cmd_reindex,
}


def dispatch(cmd):
    if not cmd:
        # This only works with argparse from Python 3, dunno how to fix it :(
        raise AbortWithHelp("Command required")

    tool = COMMANDS[cmd]
    tool()


def make_argparser():
    """Argument parsing. Keep this together with main()"""

    if PY2:
        def unicode_arg(val):
            return val.decode(sys.getfilesystemencoding() or 'utf8')
    else:
        unicode_arg = str

    # Generic options
    p_main = ArgumentParser()
    generic = p_main.add_argument_group("generic arguments")
    generic.add_argument("-q", "--quiet",
                         action='store_true', dest='quiet', default=False,
                         help="silence information messages")
    generic.add_argument("-f", "--force",
                         action='store_true', dest='force', default=False,
                         help="Kill connections automatically if they prevent a command from executing. "
                              "Rename existing databases that are in the way.")
    generic.add_argument("--traceback", action='store_true', default=False,
                         help="print traceback when an error occurs")
    generic.add_argument("--host", metavar="HOST",
                         help="hostname of database server")
    generic.add_argument("-p", "--port", metavar="PORT", type=int,
                         help="port number of database server")

    sub = p_main.add_subparsers(metavar="COMMAND", dest='cmd')

    p_cp = sub.add_parser('cp', description=cmd_copy.__doc__,
                          help="Create a copy of a database within a server")
    p_mv = sub.add_parser('mv', description=cmd_move.__doc__,
                          help="Rename a database within a server")

    for p_cmd in (p_cp, p_mv):
        p_cmd.add_argument('src', metavar="SOURCE", type=unicode_arg,
                           help="source database name")
        p_cmd.add_argument('dest', metavar="DEST", type=unicode_arg,
                           help="destination database name")
        p_cmd.add_argument("--no-backup",
                           action='store_true', dest='no_backup', default=False,
                           help="drop existing DEST database if it exists")

    p_kill = sub.add_parser('kill', description=cmd_kill.__doc__,
                            help="Terminate active connections to a database")
    p_kill.add_argument('databases', metavar="DBNAME", type=unicode_arg, nargs='+',
                        help="kill connections on this database")

    p_reindex = sub.add_parser('reindex', description=cmd_reindex.__doc__,
                               help="Gracefully recreate an index")
    p_reindex.add_argument('-d', '--database', metavar="DB", type=unicode_arg,
                           help="apply reindex in this database")
    p_reindex.add_argument('indexes', metavar="IDXNAME", type=unicode_arg, nargs='+',
                           help="reindex these indexes")

    return p_main


def main(argv=None):
    global args

    parser = make_argparser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format='%(message)s'
    )

    try:
        dispatch(args.cmd)
    except Abort as err:
        if args.traceback:
            raise
        log.fatal("Fatal: %s", err)
        if isinstance(err, AbortWithHelp):
            parser.print_help()
        sys.exit(1)

    except psycopg2.Error as err:
        if args.traceback:
            raise
        log.fatal(("PostgreSQL: %s" % err).strip())
        if getattr(err, 'pgcode', None):
            log.error("Error code: %s", err.pgcode)
        sys.exit(1)


if __name__ == '__main__':
    main()
