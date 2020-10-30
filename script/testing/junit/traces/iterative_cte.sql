CREATE TABLE tree (node INTEGER, parent INTEGER);
INSERT INTO tree VALUES (1,NULL), (10, 1), (11, 1), (100, 10), (101, 10), (110, 11), (111, 11);
WITH ITERATIVE cte(x) AS (SELECT 1 UNION ALL SELECT tree.node FROM tree INNER JOIN cte ON tree.parent=cte.x) SELECT * FROM cte;