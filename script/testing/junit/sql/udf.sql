-- udf.sql
-- Integration tests for user-defined functions.
--
-- Currently, these tests rely on the fact that we
-- utilize Postgres as a reference implementation
-- because all user-defined functions are implemented
-- in the Postgres PL/SQL dialect, PL/pgSQL.

-- Create the test table
CREATE TABLE test(id INT PRIMARY KEY, x INT);

-- Insert some data
INSERT INTO test (id, x) VALUES (0, 1), (1, 2), (2, 3);

-- Create functions
CREATE FUNCTION return_constant() RETURNS INT AS $$ \
BEGIN                                               \
  RETURN 1;                                         \
END                                                 \
$$ LANGUAGE PLPGSQL;                              

-- Invoke
SELECT x, return_constant() FROM test;
