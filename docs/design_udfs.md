# Design Doc: User-Defined Functions

### Overview

This document describes important aspects of the design and implementation of user-defined functions in NoisePage.

### Limitations

This section describes known limitations of our implementation of UDFs.

**Missing `RETURN`**

In Postgres, a PL/pgSQL function that declares a return type but is missing a `RETURN` statement in the body of the function parses successfully, but results in a runtime error when the function is executed. Currently, we fail to parse such functions (which may be directly related to the issue below).

**Implicit `RETURN`s**

Currently, the following control flow is not supported:

```sql
CREATE FUNCTION fun(x INT) RETURNS INT AS $$
BEGIN
  IF x > 10 THEN
    RETURN 0;
  ELSE
    RETURN 1;
  END IF;
END
$$ LANGUAGE PLPGSQL;
```

This fails in the parser because the library we use to parse the raw UDF (libpg_query) inserts an implicit empty `RETURN` at the end of the body of the function. This implicit `RETURN` has no associated expression, and therefore it fails when we attempt to parse it.

Obviously, we can see that this implicit `RETURN` is unreachable code, so we know this UDF body is valid.
