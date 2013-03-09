CREATE TEMPORARY TABLE "pgsqlexec_test_tab" (foo integer, bar integer);
INSERT INTO "pgsqlexec_test_tab" (foo, bar) VALUES (1,2);
INSERT INTO "pgsqlexec_test_tab" (foo, bar) VALUES (3,3);
SELECT SUM(foo) FROM "pgsqlexec_test_tab";
