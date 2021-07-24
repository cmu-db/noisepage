-- udf.sql
-- Integration tests for user-defined functions.
--
-- Currently, these tests rely on the fact that we
-- utilize Postgres as a reference implementation
-- because all user-defined functions are implemented
-- in the Postgres PL/SQL dialect, PL/pgSQL.

-- Create a test table
CREATE TABLE integers(x INT, y INT);

-- Insert some data
INSERT INTO integers (x, y) VALUES (1, 1), (2, 2), (3, 3);

-- ----------------------------------------------------------------------------
-- return_constant()

CREATE FUNCTION return_constant() RETURNS INT AS $$ \
BEGIN                                               \
  RETURN 1;                                         \
END                                                 \
$$ LANGUAGE PLPGSQL;                              

SELECT x, return_constant() FROM integers;

-- ----------------------------------------------------------------------------
-- return_input()

CREATE FUNCTION return_input(x INT) RETURNS INT AS $$ \
BEGIN                                                 \
  RETURN x;                                           \
END                                                   \
$$ LANGUAGE PLPGSQL;

SELECT x, return_input(x) FROM integers;

-- ----------------------------------------------------------------------------
-- return_sum()

CREATE FUNCTION return_sum(x INT, y INT) RETURNS INT AS $$ \
BEGIN                                                      \
  RETURN x + y;                                            \
END                                                        \
$$ LANGUAGE PLPGSQL;

SELECT x, y, return_sum(x, y) FROM integers;

-- ----------------------------------------------------------------------------
-- return_prod()

CREATE FUNCTION return_product(x INT, y INT) RETURNS INT AS $$ \
BEGIN                                                          \
  RETURN x * y;                                                \
END                                                            \
$$ LANGUAGE PLPGSQL;

SELECT x, y, return_product(x, y) FROM integers;

-- ----------------------------------------------------------------------------
-- integer_decl()

CREATE FUNCTION integer_decl() RETURNS INT AS $$ \
DECLARE                                          \
  x INT := 0;                                    \
BEGIN                                            \
  RETURN x;                                      \
END                                              \
$$ LANGUAGE PLPGSQL;

SELECT x, y, integer_decl() FROM integers; 

-- ----------------------------------------------------------------------------
-- conditional()
--
-- TODO(Kyle): The final RETURN 0 is unreachable, but we
-- need this temporary hack to deal with missing logic in parser

CREATE FUNCTION conditional(x INT) RETURNS INT AS $$ \
BEGIN                                                \
  IF x > 1 THEN                                      \
    RETURN 1;                                        \
  ELSE                                               \
    RETURN 2;                                        \
  END IF;                                            \
  RETURN 0;                                          \
END                                                  \
$$ LANGUAGE PLPGSQL;

SELECT x, conditional(x) FROM integers;

-- ----------------------------------------------------------------------------
-- proc_while()

CREATE FUNCTION proc_while() RETURNS INT AS $$ \
DECLARE                                        \
  x INT := 0;                                  \
BEGIN                                          \
  WHILE x < 10 LOOP                            \
    x = x + 1;                                 \
  END LOOP;                                    \
  RETURN x;                                    \
END                                            \
$$ LANGUAGE PLPGSQL;

SELECT x, proc_while() FROM integers;

-- ----------------------------------------------------------------------------
-- proc_fori()
--
-- TODO(Kyle): for-loop control flow (integer variant) is not supported

-- CREATE FUNCTION proc_fori() RETURNS INT AS $$ \
-- DECLARE                                       \
--   x INT := 0;                                 \
-- BEGIN                                         \
--   FOR i IN 1..10 LOOP                         \
--     x = x + 1;                                \
--   END LOOP;                                   \
--   RETURN x;                                   \
-- END                                           \
-- $$ LANGUAGE PLPGSQL;

-- SELECT x, proc_fori() FROM integers;

-- ----------------------------------------------------------------------------
-- proc_fors()
--
-- TODO(Kyle): for-loop control flow (query variant) is not supported

-- CREATE TABLE tmp(z INT);
-- INSERT INTO tmp(z) VALUES (0), (1);

-- -- Bind query result to a RECORD type
-- CREATE FUNCTION proc_fors_rec() RETURNS INT AS $$ \
-- DECLARE                                           \ 
--   x INT := 0;                                     \
--   v RECORD;                                       \
-- BEGIN                                             \
--   FOR v IN (SELECT z FROM temp) LOOP              \
--     x = x + 1;                                    \
--   END LOOP;                                       \
--   RETURN x;                                       \
-- END                                               \
-- $$ LANGUAGE PLPGSQL;

-- SELECT x, proc_fors_rec() FROM integers;

-- -- Bind query result directly to INT type
-- CREATE FUNCTION proc_fors_var() RETURNS INT AS $$ \
-- DECLARE                                           \
--   x INT := 0;                                     \
--   v INT;                                          \
-- BEGIN                                             \
--   FOR v IN (SELECT z FROM tmp) LOOP               \
--     x = x + 1;                                    \
--   END LOOP;                                       \
--   RETURN x;                                       \
-- END                                               \
-- $$ LANGUAGE PLPGSQL;

-- SELECT x, proc_fors_var() FROM integers;

CREATE FUNCTION agg_count() RETURNS INT AS $$ DECLARE v INT; BEGIN SELECT COUNT(z) INTO v FROM tmp; RETURN v; END $$ LANGUAGE PLPGSQL;
CREATE FUNCTION fun() RETURNS INT AS $$ DECLARE a INT; b INT; BEGIN SELECT COUNT(z), COUNT(z) INTO a, b FROM tmp; RETURN a + b; END $$ LANGUAGE PLPGSQL;

