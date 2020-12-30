CREATE TABLE foo (var1 INT, var2 INT, var3 INT);

INSERT INTO foo VALUES (0,0,0), (0,0,1), (0,0,2), (0,1,0), (0,1,1), (0,1,2), (0,2,0), (0,2,1), (0,2,2), (1,0,0), (1,0,1), (1,0,2), (1,1,0), (1,1,1), (1,1,2), (1,2,0), (1,2,1), (1,2,2), (2,0,0), (2,0,1), (2,0,2), (2,1,0), (2,1,1), (2,1,2), (2,2,0), (2,2,1), (2,2,2);


CREATE INDEX ind ON foo (var1, var2, var3);


-- Predicates + Sort exist
-- At least one predicate satisfied and sort not satisfied
-- Index should be chosen due to satisfied predicate, but limit should not be pushed down due to invalid order by
SELECT * FROM foo WHERE var1 >= 1 ORDER BY var3 LIMIT 17;


DROP INDEX ind;
CREATE INDEX ind ON foo (var1, var3);


-- At least one predicate satisfied and sort not satisfied
-- Index should be chosen due to satisfied predicate, but limit should not be pushed down due to invalid order by
SELECT * FROM foo WHERE var1 >= 1 AND var3 >= 1 ORDER BY var2 LIMIT 11;
SELECT * FROM foo WHERE var1 = 1 AND var3 >= 1 ORDER BY var2 LIMIT 5;
SELECT * FROM foo WHERE var1 >= 1 AND var3 >= 1 ORDER BY var3 LIMIT 11;


-- No predicates satisfied
-- Sequential scan chosen since no predicates or sort satisfied by index
SELECT * FROM foo WHERE var2 >= 1 ORDER BY var2 LIMIT 17;


-- At least one predicate satisfied and sort satisfied
-- Index can be chosen due to bounds, but limit cannot be pushed down because of complex predicate check
-- TODO(dpatra): Once predicate can be pushed down to index iterator, limit can also be pushed down
SELECT * FROM foo WHERE var1 >= 1 AND var3 >= 1 ORDER BY var1, var3 LIMIT 11;
SELECT * FROM foo WHERE var1 >= 1 AND var2 >= 1 ORDER BY var1, var3 LIMIT 11;
-- Index can be chosen due to bounds, and limit can be pushed down because of valid sort and enforced predicate check
SELECT * FROM foo WHERE var1 = 1 AND var3 >= 1 ORDER BY var3 LIMIT 5;

-- Sort exists
-- Sequential scan chosen since sort not satisfied by index
SELECT * FROM foo ORDER BY var2 LIMIT 26;
SELECT * FROM foo ORDER BY var3 LIMIT 26;


-- Sort satisfied
-- Index can be chosen due to bounds and limit pushed down due to satisfied order by and lack of predicates
SELECT * FROM foo ORDER BY var1, var3 LIMIT 26;


-- Predicates exist
-- Sequential scan chosen since predicate not satisfied by index
SELECT * FROM foo WHERE var2 >= 1 LIMIT 17;

-- Predicates satisfied
-- Index can be chosen due to bounds, but limit cannot be pushed down because of complex predicate check
-- TODO(dpatra): Once predicate can be pushed down to index iterator, limit can also be pushed down
SELECT * FROM foo WHERE var1 >= 1 AND var3 >= 1 LIMIT 11;
-- Index can be chosen due to bounds, and limit can be pushed down because of enforced predicate check
SELECT * FROM foo WHERE var1 = 1 AND var3 >= 1 LIMIT 5;


-- Neither predicates nor sort exist
-- Sequential scan chosen since no predicates nor order by's exist
SELECT * FROM foo LIMIT 26;


DROP INDEX ind;
DROP TABLE foo;
