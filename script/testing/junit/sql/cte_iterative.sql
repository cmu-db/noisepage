-- Input queries for iterative CTE integration tests.
--
-- Postgres does not support iterative CTEs. Therefore, we 
-- must manually verify the output from this trace.

CREATE TABLE tree (node INTEGER, parent INTEGER);
INSERT INTO tree VALUES (1,NULL), (10, 1), (11, 1), (100, 10), (101, 10), (110, 11), (111, 11);
WITH ITERATIVE cte(x) AS (SELECT 1 UNION ALL SELECT tree.node FROM tree INNER JOIN cte ON tree.parent=cte.x) SELECT * FROM cte;