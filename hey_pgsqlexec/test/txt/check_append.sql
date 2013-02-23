CREATE TEMPORARY TABLE "holymoly_test_tab" (foo integer, bar integer);
INSERT INTO "holymoly_test_tab" (foo, bar) VALUES (1,2);
INSERT INTO "holymoly_test_tab" (foo, bar) VALUES (3,3);
SELECT SUM(foo) FROM "holymoly_test_tab";