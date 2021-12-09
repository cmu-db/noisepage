# Design Doc: Common Table Expressions

### Overview

This document provides an overview of some of the important features of our implementation of common table expressions (CTEs).

### Design

TODO(Kyle): Fill this in.

### Limitations

Our current implementation of CTEs suffers from some known limitations which limits the queries we are able to execute. This section provides a comprehensive overview of the queries on which the system currently fails, a best-estimate of the underlying reason for the failure, and what might be required to address it.

**Loops with `UNION`**

An example query that loops as a result of a `UNION` is:

```SQL
WITH RECURSIVE x(i) AS (SELECT 1 UNION SELECT (i+1)%10 FROM x) SELECT i FROM x LIMIT 20;
```

Here we can see that the modulus operation within the inductive case of the CTE will limit the number of unique values in the table `x` to the sequence `1..9,0` at which point the sequence repeats. Naturally, with `UNION`, the fact that only these 10 unique values will be present in the table limits the size of the temporary table to 10 rows, which are the final result of the query. In NoisePage, we currently sit in an infinite loop on this input query. It appears that Postgres is able to perform some higher-level reasoning about the possible values that might appear in the inductive case of the CTE? If this is actually how they handle these cases, this seems like it would constitute a more-involved fix.

**Loops with `UNION ALL`**

Infinite loops with recursive CTEs that involve `UNION ALL` have a different underlying issue than those that loop with `UNION` alone. An example query is:

```SQL
WITH RECURSIVE x(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM x) SELECT * FROM x LIMIT 10;
```

This is another recursive CTE, but with the conspicuous absence of a predicate in the inductive case (the `SELECT` after the `UNION ALL`). Instead, the query relies on the outer `LIMIT` to limit the number of rows in the output set. This is a different underlying issue than the above because in this case the temporary table `x` does actually grow without bound, and it is _only_ the outer `LIMIT` that prevents this from being an inifinite loop.

We fail on this query as a result of the fact that we populate temporary tables for CTEs greedily, meaning that in this case we attempt to populate the temporary table `x` with an infinite number of rows and never break out of the resulting loop. It seems like there are two possible ways we might be able to address this issue:

- Push knowledge of the `LIMIT` _down_ into the iterator that is responsible for producing values from the temporary table. This way, the iterator will "know" that it should stop writing rows to the temporary table after 10 rows have been produced, elimintating the infinite loop.
- Pull tuples from the top of the query plan, rather than pushing them up from the bottom. This is a more fundamental redesign of the execution engine, and might be infeasible for that reason (i.e. it might be totally antagonistic to our implementation of pipelines). One might imagine, however, that in the "traditional" Volcano query processing style, this query would not pose an issue because the root `SELECT` operator with its `LIMIT` would only attempt to pull the next tuple from the temporary table `x` 10 times before completing the query. This would avoid the need to push knowledge of higher-level query constructs down into the CTE iterator.

**References to CTE Constructs in Outer Scope**

An example query that fails when we attempt to reference a CTE construct (e.g. a column of a temporary table)

```SQL
CREATE TABLE test (id INT PRIMARY KEY, name TEXT);
INSERT INTO test (id, name) VALUES (0, 'A')
INSERT INTO test (id, name) VALUES (1, 'B')

WITH x AS (SELECT * FROM test) SELECT * FROM x ORDER BY name;
```

This query fails with an error that reports the requested column does not exist. The column in question is the `name` column of the temporary table `x`. Interestingly enough, attempting to fix the issue with explicit column aliases for the temporary table `x` does not fix the issue:

```SQL
WITH x(id, name) AS (SELECT * FROM test) SELECT * FROM x ORDER BY name;
```

This leads me to believe that (as we will see again below) the issue lies in the order in which identifiers are visited during binding.

**Binder Temporary Table Resolution**

We fail in multiple places during binding as a result of our inability to resolve temporary tables. Example queries that highlight each of these distinct failure modes are:

```SQL
WITH RECURSIVE x(i) AS (SELECT * FROM y), y(j) AS (SELECT 1) SELECT * FROM x;
```

This query fails with an error that reports that the relation `y` does not exist.

```SQL
WITH RECURSIVE x(i) AS (SELECT 1), y(j) AS (SELECT * FROM x) SELECT * FROM y;
```

This query fails with an error that reports that the temporary table `y` has 0 columns available but 1 specified, implying that the columns of temporary table `x` have not been properly resolved.

```SQL
WITH RECURSIVE t(n) AS (SELECT (SELECT 1) UNION ALL SELECT n+1 FROM t WHERE n < 5) SELECT * FROM t;
```

This query fails with an error that reports that the CTE column type is not resolved.

In all of the above cases, the error appears because of the order in which binding of the temporary tables in the query is performed. Our current architecture for the binder performs a naive depth-first traversal of the parsed query statement during binding. However, once CTEs are introduced, this assumption is no longer valid because CTEs introduce more complex rules regarding the visibility of temporary tables across the statement.

We have an implementation in the binder now that allows us to compute a dependency graph among CTE table references within an input statement. Once this dependency graph is computed, we can establish a topoligical ordering of the temporary tables and resolve table references according to this ordering in order to avoid reference resolution failures (at least, that is the theory). Making use of this functionality will require more-invasive changes to the binder, which is why it has been deferred for the time being.

**`SELECT *` Column Ordering**

An example query that fails because of the order in which columns from `SELECT *` queries are produced is:

```SQL
WITH x(i) AS (SELECT 1), y(j) AS (SELECT 2) SELECT * FROM x INNER JOIN y ON x.i+1 = y.j;
```

We produce the correct result from this query, but fail during integration tests because we do not order the columns of the output in a way that is equivalent to the way that Postgres does it (Postgres is used as our baseline for correctness checks). This is not _really_ a correctness issue, but is still something that we might decide to take a look at in the future.
