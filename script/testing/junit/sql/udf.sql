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

CREATE TABLE temp(z INT);
INSERT INTO temp(z) VALUES (0), (1);

CREATE FUNCTION proc_fors() RETURNS INT AS $$ \
DECLARE                                       \ 
  x INT := 0;                                 \
  v RECORD;                                   \
BEGIN                                         \
  FOR v IN (SELECT z FROM temp) LOOP          \
    x = x + 1;                                \
  END LOOP;                                   \
  RETURN x;                                   \
END                                           \
$$ LANGUAGE PLPGSQL;

SELECT x, proc_fors() FROM integers;
