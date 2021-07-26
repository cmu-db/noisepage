# Design Doc: User-Defined Functions

### Overview

This document describes important aspects of the design and implementation of user-defined functions in NoisePage.

### Limitations

This section describes known limitations of our implementation of UDFs.

**Parallel Table Scans**

Consider the following function:

```sql
CREATE FUNCTION agg_count() RETURNS INT AS $$
DECLARE 
v INT; 
BEGIN 
  SELECT COUNT(z) INTO v FROM tmp; 
  RETURN v; 
END 
$$ LANGUAGE PLPGSQL;
```

Currently, we fail to generate code for this function. Code generation fails while we attempt to generate code for the embedded SQL query `SELECT COUTN(z) INTO v FROM tmp;`. The plan tree for this query is straighforward: a static aggregation with a sequential table scan as its only child. However, we fail semantic analysis for the generated code because of the presence of the ouutput callback - a TPL closure that takes the output of the query and "writes" it into the variable `v` within the contextion of the function (a simplification, but close enough). When the callback is present, we add the closure itself as an additional parameter to the top-level pipeline functions:
- Wrapper
- Init
- Run
- Teardown

This is an issue because we expect the callback function for a parallel table vector iterator to have a specific signature, and the presence of the closure violates this signature.

There are a couple of fixes available for this problem, but the question of which one is "correct" is not straightforward.
- Do we just change the signature in semantic analysis to accept this? What are the correctness implications of this decision? Is it possible that invoking a closure in parallel will result in incorrect or undefined behavior?
- It doesn't seem like we _need_ to push the closure down as an argument to all of the functions that define the pipeline, only some of them. Specifically, the closure is only used (thus far) as a mechanism for pushing results out of the query back into the function in which it is embedded. Therefore, why do we add the closure as an argument to every pipeline function? It seems like this may have been just an "expedient" solution that isn't actually what is required or desired.

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
