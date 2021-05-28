-- CTE-related integration tests from SQLite trace file `with1.test`

-----------------------------------------------------------
-- with1.test, section 1

CREATE TABLE t1(x INTEGER, y INTEGER);

WITH x(a) AS (SELECT * FROM t1) SELECT 10;

-- Postgres does not support this syntax
-- SELECT * FROM ( WITH x AS ( SELECT * FROM t1) SELECT 10);

WITH x(a) AS (SELECT * FROM t1) INSERT INTO t1 VALUES(1,2);
WITH x(a) AS (SELECT * FROM t1) DELETE FROM t1;
WITH x(a) AS (SELECT * FROM t1) UPDATE t1 SET x = y;

-----------------------------------------------------------
-- with1.test, section 2

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(x INTEGER);
INSERT INTO t1 VALUES(1);
INSERT INTO t1 VALUES(2);
WITH tmp AS (SELECT * FROM t1) SELECT x FROM tmp;

WITH tmp(a) AS (SELECT * FROM t1) SELECT a FROM tmp;

-- Postgres does not support this syntax
-- SELECT * FROM (WITH tmp(a) AS (SELECT * FROM t1) SELECT a FROM tmp);

-- Postgres does not support this syntax
-- WITH tmp1(a) AS (SELECT * FROM t1), tmp2(x) AS (SELECT * FROM tmp1) SELECT * FROM tmp2;

-- Postgres does not support this syntax
-- WITH tmp2(x) AS (SELECT * FROM tmp1), tmp1(a) AS (SELECT * FROM t1) SELECT * FROM tmp2;

-----------------------------------------------------------
-- with1.test, section 3

CREATE TABLE t3(x VARCHAR);
CREATE TABLE t4(x VARCHAR);

INSERT INTO t3 VALUES('T3');
INSERT INTO t4 VALUES('T4');

WITH t3(a) AS (SELECT * FROM t4) SELECT * FROM t3;

-- TODO: We crash on this query, Postgres returns `T4`
-- WITH tmp AS (SELECT * FROM t3), tmp2 AS (WITH tmp AS (SELECT * FROM t4) SELECT * FROM tmp) SELECT * FROM tmp2;

-- TODO: We crash on this query; Postgres returns `T3`
-- WITH tmp AS (SELECT * FROM t3), tmp2 AS (WITH xxxx AS (SELECT * FROM t4) SELECT * FROM tmp) SELECT * FROM tmp2;

-----------------------------------------------------------
-- with1.test, section 5

CREATE TABLE edge(xfrom INTEGER, xto INTEGER, seq INTEGER, PRIMARY KEY(xfrom, xto));
INSERT INTO edge VALUES(0, 1, 10);
INSERT INTO edge VALUES(1, 2, 20);
INSERT INTO edge VALUES(0, 3, 30);
INSERT INTO edge VALUES(2, 4, 40);
INSERT INTO edge VALUES(3, 4, 40);
INSERT INTO edge VALUES(2, 5, 50);
INSERT INTO edge VALUES(3, 6, 60);
INSERT INTO edge VALUES(5, 7, 70);
INSERT INTO edge VALUES(3, 7, 70);
INSERT INTO edge VALUES(4, 8, 80);
INSERT INTO edge VALUES(7, 8, 80);
INSERT INTO edge VALUES(8, 9, 90);
  
-- This fails in the parser (TargetTransform: root==null)
-- WITH RECURSIVE ancest(id, mtime) AS (VALUES(0, 0) UNION SELECT edge.xto, edge.seq FROM edge, ancest WHERE edge.xfrom=ancest.id) SELECT * FROM ancest;

-- This fails in the parser (TargetTransform: root==null)
-- WITH RECURSIVE i(x) AS (VALUES(1) UNION ALL SELECT (x+1)%10 FROM i) SELECT x FROM i LIMIT 20;

-- This fails in the parser (TargetTransform: root==null)
-- WITH RECURSIVE i(x) AS (VALUES(1) UNION SELECT (x+1)%10 FROM i) SELECT x FROM i LIMIT 20;

-----------------------------------------------------------
-- with1.test, section 6

WITH RECURSIVE x(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM x WHERE i<10) SELECT COUNT(*) FROM x;

-- terminator; the testing infrastructure chokes on trailing comments
SELECT 1;
