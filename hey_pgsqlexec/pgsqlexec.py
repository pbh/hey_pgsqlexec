#!/usr/bin/env python

"""
    hey_pgsqlexec.pgsqlexec
    ~~~~~~~~~~~~~~~~~~~~~~~~~~
    Utility for executing SQL files and strings.

    :copyright: (c) 2013 by oDesk Corporation.
    :license: BSD, see LICENSE for more details.
"""

import os
import warnings
import re

warnings.filterwarnings("ignore", "tempnam", RuntimeWarning)
warnings.filterwarnings("ignore", "tmpnam", RuntimeWarning)

def _read_file_with_localizer(fn, localizer=None):
    s = ''
    
    if localizer is None:
        s = file(fn).read()
    else:
        s = localizer.read(fn)
        
    return s

class PGSQLExecException(Exception): pass

class PGSQLExec(object):
    """
    PGSQLExec is a simple interface for dumping SQL into a PostgreSQL
    database, often from a file, and maybe getting some results out.
    
    The usage is usually something like:

      (hey_pgsqlexec.PGSQLExec(self.conn)
       .append_file('txt/check_append.sql', localizer=self._dl)
       .execute()
       .get_rows()[0][0])
    
    Essentially, PGSQLExec is a minimal, chaining wrapper around the
    psycopg2 Connection and Cursor classes to make the common case of
    working with pre-written, static SQL files easier.
    """

    def __init__(self, connection=None, output_cwd=None, name=None, cursor=None):
        self._sql = ''
        self._cwd = output_cwd

        if (connection is not None) and (cursor is not None):
            print connection
            print cursor
            raise PGSQLExecException(
                'Please only supply one of connection or cursor.')

        if connection is not None:
            if not hasattr(connection, 'cursor'):
                raise PGSQLExecException(
                    '%s does not seem to be a psycopg2 connection object.' %
                    str(connection))

            self._conn = connection
            self._cur = connection.cursor()
        elif cursor is not None:
            if not hasattr(cursor, 'connection'):
                raise PGSQLExecException(
                    '%s does not seem to be a psycopg2 cursor object.' %
                    str(cursor))

            self._conn = cursor.connection
            self._cur = cursor
        else:
            raise PGSQLExecException(
                'Please supply at least one of connection or cursor.')

        if (('psycopg2' not in str(type(self._conn))) or
            ('psycopg2' not in str(type(self._cur)))):
            raise PGSQLExecException(
                'Supplied connection/cursor is not psycopg2!')

        if output_cwd is not None:
            if name is None:
                self._csv_fn = os.path.split(os.tempnam(self._cwd))[1]
            else:
                self._csv_fn = '%s.csv' % name

            self._abs_csv_fn = os.path.join(self._cwd, self._csv_fn)

    def append_string(self, s):
        'Append a string to the SQL that will be executed.'
        self._sql += ';' + s + ';'
        return self

    def append_file(self, fn, localizer=None):
        'Append the contents of a file to the SQL that will be executed.'
        self.append_string(
            _read_file_with_localizer(fn, localizer)
            )

        return self

    def execute_to_csv_unsafe(self, override=False):
        """
        Assuming a SELECT has been appended, create a CSV file of the output.

        Note: This method is unsafe because it loads the accumulated SQL
        directly into a COPY statement.  Some efforts are made to avoid
        doing bad things, but they are minimal and can be overridden with
        the override argument.
        """
        if self._cwd is None:
            raise PGSQLExecException(
                'Requested CSV output, but output_cwd is not set!')

        query = self._sql
        query_wo_returns = re.sub(r'[\n\r]', ' ', query)
        query_wo_semicolon = re.sub(
            r'^[;\s]*', '', 
            re.sub(r'[;\s]*$', '', query_wo_returns))

        if ((not re.search(r'\s*select',
                           query_wo_semicolon, re.IGNORECASE)) and
            not override):
            raise PGSQLExecException(
                'to_csv execute only accepts one SELECT statement currently.')

        if re.search(r'(create|drop|delete|update)',
                     query_wo_semicolon, re.IGNORECASE) and not override:
            raise PGSQLExecException(
                'to_csv should not have a create, drop, delete or update.')

        self._cur.copy_expert(
            """COPY (%s) TO STDOUT WITH CSV HEADER""" % query_wo_semicolon,
            file(self._abs_csv_fn,'w'))

        return self

    def execute(self, to_csv=False, override=False):
        'Execute the accumulated SQL code that has been appended to the object.'
        self._cur.execute(self._sql)
        return self

    def commit(self):
        'Send a SQL COMMIT.'
        self._cur.connection.commit()
        return self

    def get_sql(self):
        'Get the accumulated SQL code that has been appended to the object.'
        return self._sql

    def get_csv(self, path_type):
        'Get the path to a CSV file (if any) output by this object.'
        if path_type in ('abs', 'absolute'):
            return os.path.join(self._cwd, self._csv_fn)
        elif path_type in ('rel', 'relative'):
            return self._csv_fn

        raise PGSQLExecException("Path type must be absolute or relative.")

    def get_rows(self):
        'Get the rows output by any SELECT queries executed by this object.'
        return self._cur.fetchall()
