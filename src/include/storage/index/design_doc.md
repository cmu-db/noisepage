# Index Builder

## Overview

Index is an important component in database systems and it can accelerate accessing data tables. Fast adding and dropping index will accelerate future queries
on the tables without affecting processing current queries in database systems. Thus, this component should support creating and dropping index correctly and fast
on a single or group of columns with little affect on other concurrent transactions.

## Scope

This component relies on execution engine, transaction manager and storage manager of the database systems. Since the SQL commands such as ```CREATE INDEX``` and ```DROP INDEX```
trigger creating and dropping the index, the execution engine (and parser) will help actually execute corresponding functions. Next, to build the index, values of key columns and
the pointer to target version in the version chain of a tuple are necessary. The storage manager can provide this component with methods to access those information. Moreover,
the storage manager can help this component organize the layout of index. Finally, creating and dropping index will be implemented in a transactional manner,
so transaction manager should also play a role.

This component modifies system catalogs. During creation of an index, a record containing the information about this index should be inserted into system catalogs. During deletion
of an index, the record containing the information about this index should also be deleted from system catalogs.

## Glossary (Optional)

There are no new concepts being introduced in this component.

## Architectural Design

The input of the index builder contains metadata about this index: key schema and constraint type. The output of the index builder is a [Bw-Tree](https://github.com/wangziqi2013/BwTree) index satisfying the key schema and constraints. Each element in the index is key-value pair. 
The key is a [ProjectedRow](https://github.com/cmu-db/terrier/wiki/Storage-Engine-Design#projectedrow-and-projectedcolumns) and the value is a [TupleSlot](https://github.com/cmu-db/terrier/wiki/Storage-Engine-Design#tupleslot).

### Blocking manner

A naive method to support building an index is locking the whole table to block any modifications on that table, building the index (including insertion a record into system catalogs) and releasing the lock of the table after building the index. Those operations can be done within a single transaction. The deletion of the index is a single transaction of deleting the corresponding record in system catalogs.

The challenging part is the semantics of the lock. The semantics should be
* During creation of an index, all modifications including insertion, deletion and update on the table should be blocked.
* When the system is not creating the index, all modifications including insertion, deletion and update on the table should not block one another (at least snapshot isolation).
* At any time, scan of the table should not be blocked.

After that, a shared/exclusive lock with the granularity of a table corresponds that semantics. When creation of an index starts, the transaction should first acquire the exclusive lock on that table. When the transaction completes creating the index, it releases the exclusive lock. When any modifications to the table starts, it should first acquire the shared lock on that table. When the modification completes, it releases the shared lock. All scanning operations will not come to the lock manager and read the table directly.

This design is simple and straightforward. It is also easy to implement correctly. However, it may create a huge overhead when building an index on a extremely large table.

### Non-Blocking manner

The idea of creating index without blocking modifications on the table comes from [Postgres](https://www.postgresql.org/docs/11/sql-createindex.html). We use the same idea here. The creation of an index will be completed in three transactions:
* The first transaction adds a record corresponding to the index into the system catalogs and sets the status of index to be ```INVALID```.
* The second transaction takes the first scan of the target table, builds the index and sets the status of the index in system catalogs to be ```READY``` (allow insert/update but does not allow queries) after building the index.
* The third transaction takes the second scan of the target table, adds missing parts during the first scan of the table (the second transaction) and set the status of the index in system catalogs to be ```VALID``` after building the index.

Note that before each scan of the whole table, the transaction should wait for all running transactions modifying the target table to terminate. Besides, after the second scan of the table, the (third) transaction should wait for all transactions older than it and taking snapshots of the target table to terminate. After that, the status can only be set to ```VALID```.

For uniqueness constraint, we does not allow building an index on a non-unique attribute or non-unique attributes. That means we will check the validity of the unique constraint in first transaction and abort the whole operation if violated rather than leaving an ```INVALID``` index in [Postgres](https://www.postgresql.org/docs/11/sql-createindex.html)

The big advantage of the non-blocking manner is that creating index will not block any modifications on the table, but the building process may take significantly longer time.

## Design Rationale
The blocking manner is easy to implement correctly comparing to the non-blocking manner. Thus, the blocking manner can be put into use quickly. However, we decide to choose the non-blocking implementation as our final design. First, it is easy to prove the building process is correct and we do not allow creating unique key index on non-unique attributes. Second, the most attractive point of this implementation is that creating index will not block concurrent modifications on the target table. That means it hardly affects other concurrent transactions and that is what we want. Finally, two scans of table and adding records into the index are relatively easy to implement in MVCC with the help of the storage manager and index interface. We only need to care about how to populate tuples and target columns from target tables.

## Testing Plan
The test mainly has two aspects: correctness test and performance test. For non-blocking implementation, the correctness test is easy to design. It should test whether the final result of the operation contains all attributes from all older transactions than the third transactions. The performance test may be harder since the "non-blocking" should be tested. Large table may be necessary and test the running time of those very simple transactions concurrent to transactions building the index on that large table can identify whether those simple transactions are blocked.

## Trade-offs and Potential Problems
The non-blocking implementation does not other modifications on the target table during building process but increases the time of building index significantly on large table(according to the documentation of Postgres).

The implementation discuss little about deletion in the index and it may cause problems in the second scan because it is possible to delete a record missing in the first scan but has not been added in the second scan. Similarly, there are some other consistency problems brought by non-blocking.

## Future Work
* The building process (two scans of the table) can be parallelized in a multi-threading way. That will increase the performance of the building process, especially for second and third transactions in the non-blocking implementation.
* We have a different design on creating unique index on non-unique attributes from Postgres. We need to discuss which design should be appropriate.
* The index builder can support several types of index and will select the best index given the key schema and constraint type before actual building the index.