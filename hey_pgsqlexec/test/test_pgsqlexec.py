import unittest2
import os
import re
import hey_dl
import hey_pgsqlexec
from config import get_pg_connection, get_pg_connection_string

class PGSQLExecTestCase(unittest2.TestCase):
    def setUp(self):
        self._dl = hey_dl.DirectoryLocalizer()
        self._dl.set()

        self._test_cwd = os.tempnam('/tmp')
        os.makedirs(self._test_cwd)

        self.conn = get_pg_connection()

    def tearDown(self):
        self.conn.close()

    def test_sql_string_pass_thru(self):
        assert (hey_pgsqlexec.PGSQLExec(self.conn)
                .append_string('select * from foo;')
                .get_sql().strip().find('select * from foo;')) > 0

    def test_get_rows_with_default(self):
        hey_pgsqlexec.PGSQLExec.set_default_connection(
            get_pg_connection_string())

        assert (hey_pgsqlexec.PGSQLExec()
                .append_string("SELECT 1,2,3,4,5,6,7,8,9;")
                .execute()
                .get_rows() > 5)

    def test_get_rows(self):
        assert (hey_pgsqlexec.PGSQLExec(self.conn)
                .append_string("SELECT 1,2,3,4,5,6,7,8,9;")
                .execute()
                .get_rows() > 5)

    def test_commit(self):
        (hey_pgsqlexec.PGSQLExec(self.conn)
         .append_string("""CREATE TEMPORARY TABLE "pgsqlexec_test_commit_tab" (foo integer, bar integer);""")
         .execute()
         .commit())

        (hey_pgsqlexec.PGSQLExec(self.conn)
         .append_string("""INSERT INTO "pgsqlexec_test_commit_tab" (foo, bar) VALUES (1,2);""")
         .execute()
         .commit())
        
        (hey_pgsqlexec.PGSQLExec(self.conn)
         .append_string("""INSERT INTO "pgsqlexec_test_commit_tab" (foo, bar) VALUES (3,3);""")
         .execute()
         .commit())
        
        self.assertEqual(hey_pgsqlexec.PGSQLExec(self.conn)
                         .append_string("""SELECT count(*) FROM "pgsqlexec_test_commit_tab";""")
                         .execute()
                         .get_rows()[0][0], 2)

    def test_abs_csv(self):
        (hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd)
         .append_string("""
CREATE TEMPORARY TABLE "pgsqlexec_test_commit_tab" (foo integer, bar integer);
INSERT INTO "pgsqlexec_test_commit_tab" (foo, bar) VALUES (1,2);
INSERT INTO "pgsqlexec_test_commit_tab" (foo, bar) VALUES (3,3);
""")
         .execute()
         .commit())
        
        abs_csv_fn = (
            hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd)
            .append_string("""SELECT * FROM "pgsqlexec_test_commit_tab" ORDER BY foo ASC;""")
            .execute_to_csv_unsafe()
            .get_csv('abs')
            )

        assert re.search(r'\s*1\s*,\s*2[\s\n]*3\s*,\s*3[\s\n]*', file(abs_csv_fn).read())

    def test_rel_csv(self):
        (hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd)
         .append_string("""
CREATE TEMPORARY TABLE "pgsqlexec_test_commit_tab" (foo integer, bar integer);
INSERT INTO "pgsqlexec_test_commit_tab" (foo, bar) VALUES (1,2);
INSERT INTO "pgsqlexec_test_commit_tab" (foo, bar) VALUES (3,3);
""")
         .execute()
         .commit())
        
        rel_csv_fn = (
            hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd, name='foobar')
            .append_string("""SELECT * FROM "pgsqlexec_test_commit_tab" ORDER BY foo ASC;""")
            .execute_to_csv_unsafe()
            .get_csv('rel')
            )

        self.assertEqual(rel_csv_fn, 'foobar.csv')

    def test_init_exceptions(self):
        with self.assertRaises(hey_pgsqlexec.PGSQLExecException):
            hey_pgsqlexec.PGSQLExec(1)

        with self.assertRaises(hey_pgsqlexec.PGSQLExecException):
            hey_pgsqlexec.PGSQLExec(connection=1)

        with self.assertRaises(hey_pgsqlexec.PGSQLExecException):
            hey_pgsqlexec.PGSQLExec(cursor=1)

    def test_abs_or_rel(self):
        (hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd)
         .append_string("""
CREATE TEMPORARY TABLE "pgsqlexec_test_commit_tab" (foo integer, bar integer);
INSERT INTO "pgsqlexec_test_commit_tab" (foo, bar) VALUES (1,2);
INSERT INTO "pgsqlexec_test_commit_tab" (foo, bar) VALUES (3,3);
""")
         .execute()
         .commit())

        with self.assertRaises(hey_pgsqlexec.PGSQLExecException):
            (hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd, name='foobar')
             .append_string("""SELECT * FROM "pgsqlexec_test_commit_tab" ORDER BY foo ASC;""")
             .execute_to_csv_unsafe()
             .get_csv('what')
             )
            
    def test_csv_safety_net(self):
        (hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd)
         .append_string("""
CREATE TEMPORARY TABLE "pgsqlexec_test_commit_tab" (foo integer, bar integer);
INSERT INTO "pgsqlexec_test_commit_tab" (foo, bar) VALUES (1,2);
INSERT INTO "pgsqlexec_test_commit_tab" (foo, bar) VALUES (3,3);
""")
         .execute()
         .commit())
        
        with self.assertRaises(hey_pgsqlexec.PGSQLExecException):
            (hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd, name='foo1')
             .append_string("""CREATE TEMPORARY TABLE "foo" AS SELECT * FROM pgsqlexec_test_commit_tab""")
             .execute_to_csv_unsafe()
             )

        with self.assertRaises(hey_pgsqlexec.PGSQLExecException):
            (hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd, name='foo2')
             .append_string("""UPDATE pgsqlexec_test_commit_tab SET foo=7""")
             .execute_to_csv_unsafe()
             )

        with self.assertRaises(hey_pgsqlexec.PGSQLExecException):
            (hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd, name='foo3')
             .append_string("""DELETE FROM pgsqlexec_test_commit_tab""")
             .execute_to_csv_unsafe()
             )

    def test_csv_safety_net_override(self):
        (hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd)
         .append_string("""
CREATE TEMPORARY TABLE "pgsqlexec_test_create_tab" (foo integer, bar integer);
INSERT INTO "pgsqlexec_test_create_tab" (foo, bar) VALUES (1,2);
INSERT INTO "pgsqlexec_test_create_tab" (foo, bar) VALUES (3,3);
""")
         .execute()
         .commit())
        
        (hey_pgsqlexec.PGSQLExec(self.conn, output_cwd=self._test_cwd, name='foo2')
         .append_string("""SELECT foo FROM pgsqlexec_test_create_tab""")
         .execute_to_csv_unsafe(override=True)
         )
        
    def test_append_file_get_rows(self):
        assert (hey_pgsqlexec.PGSQLExec(self.conn)
                .append_file('txt/check_append.sql', localizer=self._dl)
                .execute()
                .get_rows()[0][0]) == 4

    def test_append_dir_get_rows(self):
        self.assertEqual(
            len(
                hey_pgsqlexec.PGSQLExec(self.conn)
                .append_dir('txt/sql_dir', localizer=self._dl)
                .append_string(
                    """SELECT * FROM pgsqlexec_test_tab1 NATURAL JOIN pgsqlexec_test_tab2""")
                .execute()
                .get_rows()), 2)

