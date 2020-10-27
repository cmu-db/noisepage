# Discussion: DDL Implementation

## Current State

### Transactional Catalog

The existing transactional catalog solves several concurrency problems for us by leveraging the existing MVCC semantics of the system.  Specifically, many concurrent DDL conflicts (name collisions, modifying the same table, etc.) are solved by our existing write-write and index conflict detection mechanisms.  Therefore, the primary challenge we face is managing physical representations of logical elements (compiled functions, etc.) and implicit write-write conflicts (adding an index while another transaction modifies the underlying table).

### Lazy DDL at Storage Layer

There was a proof of concept of how to implement this from last spring.  However, the implementation has fallen significantly behind the current version of terrier.  It was performant enough that it should probably serve as the reference point for implementing DDL at the storage layer, but much of the actual code has been obsoleted.  The specific design points to bring forward are:
1. Fast-path for single version operations (common case)
2. Header mangling for projections (fast translation between versions)
3. Partial serialization of transactions (all transactions can be labeled as before or after a given DDL change)
4. SqlTable applies appropriate default values to "old" tables

## Challenges

### Add index
The primary challenge on `add index` is inserting existing data in a way that does not cause spurious failures in concurrent transactions while still ensuring a fully consistent index in the end.  This also implies that we should be able to guarantee that the operation will terminate at some point.

Possible solution:  The lazy schema change implementation expected a system for forcing arbitrary validation or work during the commit section of a transaction.  We may be able to use this to allow concurrent transactions to do forward work during the process.

- Concurrent Index DDL and Table DDL
Since we plan on compiling key-building functions at index creation time, we need to ensure that visible functions are always in lock-step with the visible table.

There are two possible solutions: 1) index DDL always grabs a write-lock on the indexed table, and 2) adding a shared-lock mechanism to tables.  The first solution is the simplest to implement and leverages existing code the most, but it greatly limits concurrency to one transaction at a time per table (even if only indexes are being modified).  The second solution allows more concurrency on indexing at the cost of more logic and checks elsewhere.

Recommend solution (1) since the common case does not include concurrent DDL.

- Maintaining logical-physical consistency