CREATE TABLE foo (var1 INT, var2 INT, var3 INT);

INSERT INTO foo VALUES (0,0,0), (0,0,1), (0,0,2), (0,1,0), (0,1,1), (0,1,2), (0,2,0), (0,2,1), (0,2,2), (1,0,0), (1,0,1), (1,0,2), (1,1,0), (1,1,1), (1,1,2), (1,2,0), (1,2,1), (1,2,2), (2,0,0), (2,0,1), (2,0,2), (2,1,0), (2,1,1), (2,1,2), (2,2,0), (2,2,1), (2,2,2);


CREATE INDEX ind ON foo (var1, var2, var3);


-- Predicates + Sort exist
-- At least one predicate satisfied and sort not satisfied
SELECT * FROM foo WHERE var1 >= 1 ORDER BY var3 LIMIT 17;

DROP INDEX ind;
CREATE INDEX ind ON foo (var1, var3);

-- No predicates satisfied
SELECT * FROM foo WHERE var2 >= 1 ORDER BY var2 LIMIT 17;

-- At least one predicate satisfied and sort satisfied
-- Predicate must be checked
SELECT * FROM foo WHERE var1 >= 1 AND var3 >= 1 ORDER BY var1, var3 LIMIT 11;
SELECT * FROM foo WHERE var1 >= 1 AND var2 >= 1 ORDER BY var1, var3 LIMIT 11;
SELECT * FROM foo WHERE var1 >= 1 AND var3 >= 1 ORDER BY var2 LIMIT 11;
SELECT * FROM foo WHERE var1 >= 1 AND var3 >= 1 ORDER BY var3 LIMIT 11;


-- Sort exists
-- Sort not satisfied
SELECT * FROM foo ORDER BY var2 LIMIT 26;
SELECT * FROM foo ORDER BY var3 LIMIT 26;

-- Sort satisfied
SELECT * FROM foo ORDER BY var1, var3 LIMIT 26;


-- Predicates exist
-- Predicates not satisfied
SELECT * FROM foo WHERE var2 >= 1 LIMIT 17;

-- Predicates satisfied
-- Predicate must be checked
SELECT * FROM foo WHERE var1 >= 1 AND var3 >= 1 LIMIT 11;


-- Neither predicates nor sort exist
SELECT * FROM foo LIMIT 26;


DROP INDEX ind;
DROP TABLE foo;
