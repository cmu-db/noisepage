-- CTE-related integration tests from SQLite trace file `with2.test`

-----------------------------------------------------------
-- with2.test, section 1

CREATE TABLE t1(a INTEGER);
INSERT INTO t1 VALUES(1);
INSERT INTO t1 VALUES(2);

WITH x1 AS (SELECT * FROM t1) SELECT sum(a) FROM x1;

-- We crash on this query; Postgres succeeds
-- WITH x1 AS (SELECT * FROM t1) SELECT (SELECT sum(a) FROM x1);

-- We crash on this query; Postgres succeeds
-- WITH x1 AS (SELECT * FROM t1) SELECT (SELECT sum(a) FROM x1), (SELECT max(a) FROM x1);

-- This fails in the parser (TargetTransform==null)
-- WITH RECURSIVE t4(x) AS (VALUES(4) UNION ALL SELECT x+1 FROM t4 WHERE x<10) SELECT * FROM t4;

-- terminator; the testing infrastructure chokes on trailing comments
SELECT 1;
