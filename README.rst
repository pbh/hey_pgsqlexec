hey_pgsqlexec
=============
A simple executable object for dumping SQL from files into a PostgreSQL
database through psycopg2.

__doc__
-------
PGSQLExec is a simple interface for dumping SQL into a PostgreSQL
database, often from a file, and maybe getting some results out.

The usage is usually something like:

.. code:: python

 (hey_pgsqlexec.PGSQLExec(self.conn)
  .append_file('txt/check_append.sql', localizer=self._dl)
  .execute()
  .get_rows()[0][0])

Essentially, PGSQLExec is a minimal, chaining wrapper around the
psycopg2 Connection and Cursor classes to make the common case of
working with pre-written, static SQL files easier.
