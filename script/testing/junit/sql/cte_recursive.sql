# Input queries for recursive CTE integration tests.

# Baseline recursive CTE
CREATE TABLE tree (node INTEGER, parent INTEGER);
INSERT INTO tree VALUES (1,NULL), (10, 1), (11, 1), (100, 10), (101, 10), (110, 11), (111, 11);
WITH RECURSIVE cte(x) AS (SELECT 1 UNION ALL SELECT tree.node FROM tree INNER JOIN cte ON tree.parent=cte.x) SELECT * FROM cte;