-- Input queries for recursive CTE integration tests.

-----------------------------------------------------------
-- Hand-Rolled Test Cases

-- Baseline recursive CTE
CREATE TABLE tree (node INTEGER, parent INTEGER);
INSERT INTO tree VALUES (1,NULL), (10, 1), (11, 1), (100, 10), (101, 10), (110, 11), (111, 11);
WITH RECURSIVE cte(x) AS (SELECT 1 UNION ALL SELECT tree.node FROM tree INNER JOIN cte ON tree.parent=cte.x) SELECT * FROM cte;

-----------------------------------------------------------
-- Adapted from Postgres Regression Test (`with.sql`)
-- https://github.com/postgres/postgres/blob/master/src/test/regress/sql/with.sql

-- sum of 1..100
-- TODO: Fails in parser (TargetTransform root==null)
-- WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 100) SELECT sum(n) FROM t;

-- TODO: Fails in parser (TargetTransform root==null)
-- WITH RECURSIVE t(n) AS (SELECT (VALUES(1)) UNION ALL SELECT n+1 FROM t WHERE n < 5) SELECT * FROM t;

-- This is an infinite loop with UNION ALL, but not with UNION
-- TODO: We loop infinitely on this
-- WITH RECURSIVE t(n) AS (SELECT 1 UNION SELECT 10-n FROM t) SELECT * FROM t;

-- This'd be an infinite loop, but outside query reads only as much as needed
-- TODO: Fails in parser (TargetTransform root==null)
-- WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM t) SELECT * FROM t LIMIT 10;

-- TODO: Fails in parser
-- WITH RECURSIVE y (id) AS (VALUES (1)), x (id) AS (SELECT * FROM y UNION ALL SELECT id+1 FROM x WHERE id < 5) SELECT * FROM x;

-- TODO: Fails in parser
-- WITH RECURSIVE x(id) AS (SELECT * FROM y UNION ALL SELECT id+1 FROM x WHERE id < 5), y(id) AS (values (1)) SELECT * FROM x;

-- subquery
-- TODO: Fails in parser
-- WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n IN (SELECT * FROM x)) SELECT * FROM x;

CREATE TABLE department (id INTEGER PRIMARY KEY, parent_department INTEGER, name TEXT);

INSERT INTO department VALUES (0, NULL, 'ROOT');
INSERT INTO department VALUES (1, 0, 'A');
INSERT INTO department VALUES (2, 1, 'B');
INSERT INTO department VALUES (3, 2, 'C');
INSERT INTO department VALUES (4, 2, 'D');
INSERT INTO department VALUES (5, 0, 'E');
INSERT INTO department VALUES (6, 4, 'F');
INSERT INTO department VALUES (7, 5, 'G');

-- extract all departments under 'A'. Result should be A, B, C, D and F
-- TODO: Crashes the DBMS
-- WITH RECURSIVE subdepartment AS (SELECT name as root_name, * FROM department WHERE name = 'A' UNION ALL SELECT sd.root_name, d.* FROM department AS d, subdepartment AS sd WHERE d.parent_department = sd.id) SELECT * FROM subdepartment ORDER BY name;

-- handle the case were recursive structure is ignored
-- TODO: Crashes the DBMS
-- WITH RECURSIVE subdepartment AS (SELECT * FROM department WHERE name = 'A') SELECT * FROM subdepartment ORDER BY name;

-- Terminator; TracefileTest chokes on trailing comments
SELECT 1;
