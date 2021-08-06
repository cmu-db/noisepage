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

SELECT return_constant();

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

SELECT integer_decl(); 

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

SELECT proc_while();

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
-- sql_select_single_constant()

CREATE FUNCTION sql_select_single_constant() RETURNS INT AS $$ \
DECLARE                                                        \
  v INT;                                                       \
BEGIN                                                          \
  SELECT 1 INTO v;                                             \
  RETURN v;                                                    \
END                                                            \
$$ LANGUAGE PLPGSQL;

SELECT sql_select_single_constant();

-- ----------------------------------------------------------------------------
-- sql_select_mutliple_constants()

CREATE FUNCTION sql_select_mutliple_constants() RETURNS INT AS $$ \
DECLARE                                                           \
  x INT;                                                          \
  y INT;                                                          \
BEGIN                                                             \
  SELECT 1, 2 INTO x, y;                                          \
  RETURN x + y;                                                   \
END                                                               \
$$ LANGUAGE PLPGSQL;

SELECT sql_select_mutliple_constants();

-- ----------------------------------------------------------------------------
-- sql_select_constant_assignment()

CREATE FUNCTION sql_select_constant_assignment() RETURNS INT AS $$ \
DECLARE                                                            \
  x INT;                                                           \
  y INT;                                                           \
BEGIN                                                              \
  x = (SELECT 1);                                                  \
  y = (SELECT 2);                                                  \
  RETURN x + y;                                                    \
END                                                                \
$$ LANGUAGE PLPGSQL;

SELECT sql_select_constant_assignment();

-- ----------------------------------------------------------------------------
-- sql_embedded_agg_count()

CREATE FUNCTION sql_embedded_agg_count() RETURNS INT AS $$ \
DECLARE                                                    \
  v INT;                                                   \
BEGIN                                                      \
  SELECT COUNT(*) FROM integers INTO v;                    \
  RETURN v;                                                \
END                                                        \
$$ LANGUAGE PLPGSQL;

SELECT sql_embedded_agg_count();

-- ----------------------------------------------------------------------------
-- sql_embedded_agg_min()

CREATE FUNCTION sql_embedded_agg_min() RETURNS INT AS $$   \
DECLARE                                                    \
  v INT;                                                   \
BEGIN                                                      \
  SELECT MIN(x) FROM integers INTO v;                      \
  RETURN v;                                                \
END                                                        \
$$ LANGUAGE PLPGSQL;

SELECT sql_embedded_agg_min();

-- ----------------------------------------------------------------------------
-- sql_embedded_agg_max()

CREATE FUNCTION sql_embedded_agg_max() RETURNS INT AS $$   \
DECLARE                                                    \
  v INT;                                                   \
BEGIN                                                      \
  SELECT MAX(x) FROM integers INTO v;                      \
  RETURN v;                                                \
END                                                        \
$$ LANGUAGE PLPGSQL;

SELECT sql_embedded_agg_max();

-- ----------------------------------------------------------------------------
-- sql_embedded_agg_multi()

CREATE FUNCTION sql_embedded_agg_multi() RETURNS INT AS $$ \
DECLARE                                                    \
  minimum INT;                                             \
  maximum INT;                                             \
BEGIN                                                      \
  minimum = (SELECT MIN(x) FROM integers);                 \
  maximum = (SELECT MAX(x) FROM integers);                 \
  RETURN minimum + maximum;                                \
END;                                                       \
$$ LANGUAGE PLPGSQL;

-- ----------------------------------------------------------------------------
-- proc_fors_constant_var()

-- Select constant into a scalar variable
CREATE FUNCTION proc_fors_constant_var() RETURNS INT AS $$ \
DECLARE                                                    \
  v INT;                                                   \
  x INT := 0;                                              \
BEGIN                                                      \
  FOR v IN SELECT 1 LOOP                                   \
    x = x + 1;                                             \
  END LOOP;                                                \
  RETURN x;                                                \
END                                                        \
$$ LANGUAGE PLPGSQL;

SELECT proc_fors_constant_var();

-- ----------------------------------------------------------------------------
-- proc_fors_constant_vars()

-- Select multiple constants in scalar variables
CREATE FUNCTION proc_fors_constant_vars() RETURNS INT AS $$ \
DECLARE                                                     \
  x INT;                                                    \
  y INT;                                                    \
  z INT := 0;                                               \
BEGIN                                                       \
  FOR x, y IN SELECT 1, 2 LOOP                              \
    z = z + 1;                                              \
  END LOOP;                                                 \
  RETURN z;                                                 \
END                                                         \
$$ LANGUAGE PLPGSQL;

SELECT proc_fors_constant_vars();

-- ----------------------------------------------------------------------------
-- proc_fors_rec()
--
-- TODO(Kyle): RECORD types not supported

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

-- SELECT proc_fors_rec() FROM integers;

-- ----------------------------------------------------------------------------
-- proc_fors_var()

-- Bind query result directly to INT type
CREATE FUNCTION proc_fors_var() RETURNS INT AS $$ \
DECLARE                                           \
  c INT := 0;                                     \
  v INT;                                          \
BEGIN                                             \
  FOR v IN (SELECT x FROM integers) LOOP          \
    c = c + 1;                                    \
  END LOOP;                                       \
  RETURN c;                                       \
END                                               \
$$ LANGUAGE PLPGSQL;

SELECT proc_fors_var();
