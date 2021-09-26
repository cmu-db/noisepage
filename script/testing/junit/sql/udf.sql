-- udf.sql
-- Integration tests for user-defined functions.
--
-- Currently, these tests rely on the fact that we
-- utilize Postgres as a reference implementation
-- because all user-defined functions are implemented
-- in the Postgres PL/SQL dialect, PL/pgSQL.

-- Create test tables
CREATE TABLE integers(x INT, y INT);
INSERT INTO integers(x, y) VALUES (1, 1), (2, 2), (3, 3);

CREATE TABLE strings(s TEXT);
INSERT INTO strings(s) VALUES ('aaa'), ('bbb'), ('ccc');

-- ----------------------------------------------------------------------------
-- return_constant()

CREATE FUNCTION return_constant() RETURNS INT AS $$ \
BEGIN                                               \
  RETURN 1;                                         \
END                                                 \
$$ LANGUAGE PLPGSQL;                              

SELECT return_constant();

DROP FUNCTION return_constant();

CREATE FUNCTION return_constant_str() RETURNS TEXT AS $$ \
BEGIN                                                \
  RETURN 'hello, functions';                         \
END                                                  \
$$ LANGUAGE PLPGSQL;

SELECT return_constant_str();

DROP FUNCTION return_constant_str();

-- ----------------------------------------------------------------------------
-- return_input()

CREATE FUNCTION return_input(x INT) RETURNS INT AS $$ \
BEGIN                                                 \
  RETURN x;                                           \
END                                                   \
$$ LANGUAGE PLPGSQL;

SELECT x, return_input(x) FROM integers;

DROP FUNCTION return_input(INT);

CREATE FUNCTION return_input(x TEXT) RETURNS TEXT AS $$ \
BEGIN                                                   \
  RETURN x;                                             \
END                                                     \
$$ LANGUAGE PLPGSQL;

SELECT s, return_input(s) FROM strings;

DROP FUNCTION return_input(TEXT);

-- ----------------------------------------------------------------------------
-- return_sum()

CREATE FUNCTION return_sum(x INT, y INT) RETURNS INT AS $$ \
BEGIN                                                      \
  RETURN x + y;                                            \
END                                                        \
$$ LANGUAGE PLPGSQL;

SELECT x, y, return_sum(x, y) FROM integers;

DROP FUNCTION return_sum(INT, INT);

-- ----------------------------------------------------------------------------
-- return_prod()

CREATE FUNCTION return_product(x INT, y INT) RETURNS INT AS $$ \
BEGIN                                                          \
  RETURN x * y;                                                \
END                                                            \
$$ LANGUAGE PLPGSQL;

SELECT x, y, return_product(x, y) FROM integers;

DROP FUNCTION return_product(INT, INT);

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

DROP FUNCTION integer_decl();

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

DROP FUNCTION conditional(INT);

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

DROP FUNCTION proc_while();

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

DROP FUNCTION sql_select_single_constant();

-- ----------------------------------------------------------------------------
-- sql_select_mutliple_constants()

CREATE FUNCTION sql_select_multiple_constants() RETURNS INT AS $$ \
DECLARE                                                           \
  x INT;                                                          \
  y INT;                                                          \
BEGIN                                                             \
  SELECT 1, 2 INTO x, y;                                          \
  RETURN x + y;                                                   \
END                                                               \
$$ LANGUAGE PLPGSQL;

SELECT sql_select_multiple_constants();

DROP FUNCTION sql_select_multiple_constants();

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

DROP FUNCTION sql_select_constant_assignment();

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

DROP FUNCTION sql_embedded_agg_count();

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

DROP FUNCTION sql_embedded_agg_min();

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

DROP FUNCTION sql_embedded_agg_max();

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

DROP FUNCTION sql_embedded_agg_multi();

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

DROP FUNCTION proc_fors_constant_var();

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

DROP FUNCTION proc_fors_constant_vars();

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

DROP FUNCTION proc_fors_var();

-- ----------------------------------------------------------------------------
-- proc_call_*()

CREATE FUNCTION proc_call_callee() RETURNS INT AS $$ \
BEGIN                                                \
  RETURN 1;                                          \
END                                                  \
$$ LANGUAGE PLPGSQL;

-- Just RETURN the result of call
CREATE FUNCTION proc_call_ret() RETURNS INT AS $$ \
BEGIN                                             \
  RETURN proc_call_callee();                      \
END                                               \
$$ LANGUAGE PLPGSQL;

SELECT proc_call_ret();

-- Assign the result of call to variable
CREATE FUNCTION proc_call_assign() RETURNS INT AS $$ \
DECLARE                                              \
  v INT;                                             \
BEGIN                                                \
  v = proc_call_callee();                            \
  RETURN v;                                          \
END                                                  \
$$ LANGUAGE PLPGSQl;

SELECT proc_call_assign();

-- SELECT the result of call into variable
CREATE FUNCTION proc_call_select() RETURNS INT AS $$ \
DECLARE                                              \
  v INT;                                             \
BEGIN                                                \
  SELECT proc_call_callee() INTO v;                  \
  RETURN v;                                          \
END                                                  \
$$ LANGUAGE PLPGSQL;

SELECT proc_call_select();

DROP FUNCTION proc_call_callee();
DROP FUNCTION proc_call_ret();
DROP FUNCTION proc_call_assign();
DROP FUNCTION proc_call_select();

-- ----------------------------------------------------------------------------
-- proc_predicate()

CREATE FUNCTION proc_predicate(threshold INT) RETURNS INT AS $$ \
DECLARE                                                         \
  c INT;                                                        \
BEGIN                                                           \
  SELECT COUNT(x) FROM integers WHERE x > threshold INTO c;     \
  RETURN c;                                                     \
END                                                             \
$$ LANGUAGE PLPGSQL;                                            

SELECT proc_predicate(0);
SELECT proc_predicate(1);
SELECT proc_predicate(2);

DROP FUNCTION proc_predicate(INT);

-- ----------------------------------------------------------------------------
-- proc_call_args()

-- Argument to call can be an expression
CREATE FUNCTION proc_call_args() RETURNS INT AS $$ \
DECLARE                                            \
  x INT := 1;                                      \
  y INT := 2;                                      \
  z INT := 3;                                      \
BEGIN                                              \
  RETURN ABS(x * y + z);                           \
END                                                \
$$ LANGUAGE PLPGSQL;

SELECT proc_call_args();

DROP FUNCTION proc_call_args();

-- Argument to call can be an identifier
CREATE FUNCTION proc_call_args() RETURNS INT AS $$ \
DECLARE                                            \
  x INT := 1;                                      \
  y INT := 2;                                      \
  z INT := 3;                                      \
  r INT;                                           \
BEGIN                                              \
  r = x * y + z;                                   \
  RETURN ABS(r);                                   \
END                                                \
$$ LANGUAGE PLPGSQL;

SELECT proc_call_args();

DROP FUNCTION proc_call_args();

-- ----------------------------------------------------------------------------
-- proc_promotion()

-- Able to (silently) promote REAL to DOUBLE PRECISION
CREATE FUNCTION proc_promotion() RETURNS REAL AS $$ \
DECLARE                                             \
  x INT := 1;                                       \
  y REAL := 1.0;                                    \
  t REAL;                                           \
BEGIN                                               \
  t = x * y;                                        \
  RETURN FLOOR(t);                                  \
END                                                 \
$$ LANGUAGE PLPGSQL; 

SELECT proc_promotion();
DROP FUNCTION proc_promotion();

-- Able to (silently) promote FLOAT to DOUBLE PRECISION
CREATE FUNCTION proc_promotion() RETURNS FLOAT AS $$ \
DECLARE                                              \
  x INT := 1;                                        \
  y FLOAT := 1.0;                                    \
  t FLOAT;                                           \
BEGIN                                                \
  t = x * y;                                         \
  RETURN FLOOR(t);                                   \
END                                                  \
$$ LANGUAGE PLPGSQL; 

SELECT proc_promotion();
DROP FUNCTION proc_promotion();

-- Promotion does not affect correct operation of DOUBLE PRECISION
CREATE FUNCTION proc_promotion() RETURNS DOUBLE PRECISION AS $$ \
DECLARE                                                         \
  x INT := 1;                                                   \
  y DOUBLE PRECISION := 1.0;                                    \
  t DOUBLE PRECISION;                                           \
BEGIN                                                           \
  t = x * y;                                                    \
  RETURN FLOOR(t);                                              \
END                                                             \
$$ LANGUAGE PLPGSQL; 

SELECT proc_promotion();
DROP FUNCTION proc_promotion();

-- Promotion does not affect correct operation of FLOAT8
CREATE FUNCTION proc_promotion() RETURNS DOUBLE PRECISION AS $$ \
DECLARE                                                         \
  x INT := 1;                                                   \
  y DOUBLE PRECISION := 1.0;                                    \
  t DOUBLE PRECISION;                                           \
BEGIN                                                           \
  t = x * y;                                                    \
  RETURN FLOOR(t);                                              \
END                                                             \
$$ LANGUAGE PLPGSQL; 

SELECT proc_promotion();
DROP FUNCTION proc_promotion();

-- Promotion works as expected with UDF arguments
CREATE FUNCTION proc_promotion(x FLOAT) RETURNS FLOAT AS $$ \
BEGIN                                                       \
  RETURN x;                                                 \
END                                                         \
$$ LANGUAGE PLPGSQL; 

SELECT proc_promotion(1337.0);
DROP FUNCTION proc_promotion(FLOAT);

-- Promotion works as expected with UDF arguments
CREATE FUNCTION proc_promotion(x REAL) RETURNS REAL AS $$ \
BEGIN                                                     \
  RETURN x;                                               \
END                                                       \
$$ LANGUAGE PLPGSQL; 

SELECT proc_promotion(1337.0);
DROP FUNCTION proc_promotion(REAL);

-- Promotion works as expected with UDF arguments
CREATE FUNCTION proc_promotion(x DOUBLE PRECISION) RETURNS DOUBLE PRECISION AS $$ \
BEGIN                                                                             \
  RETURN x;                                                                       \
END                                                                               \
$$ LANGUAGE PLPGSQL; 

SELECT proc_promotion(1337.0);
DROP FUNCTION proc_promotion(DOUBLE PRECISION);

-- Promotion works as expected with UDF arguments
CREATE FUNCTION proc_promotion(x FLOAT8) RETURNS FLOAT8 AS $$ \
BEGIN                                                         \
  RETURN x;                                                   \
END                                                           \
$$ LANGUAGE PLPGSQL; 

SELECT proc_promotion(1337.0);
DROP FUNCTION proc_promotion(FLOAT8);

-- ----------------------------------------------------------------------------
-- proc_cast()

-- CAST works in assignment expression
CREATE FUNCTION proc_cast() RETURNS FLOAT AS $$ \
DECLARE                                         \
  x FLOAT;                                      \
BEGIN                                           \
  x = CAST(1 AS FLOAT);                         \
  RETURN x;                                     \
END                                             \
$$ LANGUAGE PLPGSQL;

SELECT proc_cast();
DROP FUNCTION proc_cast();

-- TODO(Kyle): this is a great example of a function that
-- we can't currently compile because we only resort to a
-- full handoff to the SQL execution infrastructure in the
-- case of assignment expressions. For everything else, in 
-- this case a RETURN statement, we don't yet have the
-- ability to defer this to the SQL engine, and we also can't
-- handle it in the "builtin" manner, so we just fail.

-- CREATE FUNCTION proc_cast() RETURNS FLOAT AS $$ \
-- BEGIN                                           \
--   RETURN CAST(1 AS FLOAT);                      \
-- END                                             \
-- $$ LANGUAGE PLPGSQL;

-- ----------------------------------------------------------------------------
-- proc_is_null()

CREATE FUNCTION proc_is_null(x INT) RETURNS INT AS $$ \
DECLARE                                               \
  r INT;                                              \
BEGIN                                                 \
  IF x IS NULL THEN                                   \
    r = 1;                                            \
  ELSE                                                \
    r = 2;                                            \
  END IF;                                             \
  RETURN r;                                           \
END                                                   \
$$ LANGUAGE PLPGSQL;

SELECT proc_is_null(1);
DROP FUNCTION proc_is_null(INT);

CREATE FUNCTION proc_is_null(x INT) RETURNS INT AS $$ \
DECLARE                                               \
  r INT;                                              \
BEGIN                                                 \
  IF x IS NOT NULL THEN                               \
    r = 1;                                            \
  ELSE                                                \
    r = 2;                                            \
  END IF;                                             \
  RETURN r;                                           \
END                                                   \
$$ LANGUAGE PLPGSQL;

SELECT proc_is_null(1);
DROP FUNCTION proc_is_null(INT);
