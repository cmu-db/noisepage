#  Project Design Document: Concurrent Schema Change

## Overview

Each table in a DBMS has a schema, which specifies how many columns are in the table, and the constraints on each column (type, nullable, default value). A schema change can involving adding/dropping columns, or updating column constraints. Many DBMSs (PostgreSQL, SQLite, and RocksDB) block the table when they perform a schema change on a table. This causes the table to be unavailable for a considerable duration, which is undesirable for systems requiring high availability. Terrier currently does not support schema change. 

The goal of this project is to add schema change functionality to Terrier, do it in a non-blocking, lazy way, and add support for the ALTER TABLE SQL command. The idea is to store a separate Datatable for each schema version. Our goals are:

75%: Support lazy add/drop column and arbitrary number of schema versions (DONE)

100%: Support ALTER TABLE command, including add/drop column, change column type, change column default value (add/drop column DONE)

125%: Do background migration of tupleslots. Support GC of old schema versions and datatables. 

150%: Support unsafe schema changes, which includes updating type, nullable, or default value status of columns. Codegen layer to precompile tuple transformations.

We have yet to design our system for unsafe schema changes and precompilation. In this design doc we will focus on the 75% to 125% goals.

## Scope

Our project will mainly modify Sqltable and Catalog. Sqltable will be modified to handle multiple datatables . The datatables are transparent to the users of Sqltable,  who view Sqltable as a logical table with multiple versions. All functions in the Sqltable API (such as insert/delete/update/select/scan/begin/end/projectmapforoids) are augmented with an extra argument, layout_version. layout_version tells Sqltable which version of the table the function is targeted to.

We will modify the Catalog to support the functions DatabaseCatalog::UpdateSchema()/GetSchema()/GetConstraints(). The catalog will be store information about the schema versions of each Sqltable, and use the timestamp of a transaction to infer the correct schema version of each Sqltable the transaction accesses. 

We will not change the API of Datatable, and make only minor additions to it. For convenience, within each Datatable we store its corresponding version, default value map, column id to oid map, and column oid to id map (explained below). Similarly, we will not be touching Tupleslots, Rawblocks, or Index.

We will make changes to the parser, binder, optimizer, planner, and execution layers, to support different types of ALTER TABLE SQL commands.

## Architectural Design

### SqlTable

To manage multiple datatables, each Sqltable keeps a ordered map (tables_) from version number to a struct containing the metadata of each datatable (called DataTableVersion). 

Note that in the vast majority of cases, there will be no schema changes, and each Sqltable manages only one Datatable. If we need to go through tables_, which is a concurrent map (or vector), on each call to Sqltable, that will be a huge overhead. Therefore, we will cache the DataTable in the SqlTable when there is only one DataTable to bypass the map/vector lookup.

Each Sqltable has a separate version number counter, managed by the catalog. Each Sqltable starts out with version number 0, and manages a single datatable. When UpdateSchema() is called, the version number is incremented, and we create a new datatable via SqlTable::CreateTable(), which is the intended location for all tuples under the new schema. Note that within a Sqltable, each datatable uniquely corresponds to a version number and a schema, both of which never change. Therefore, in each datatable, we store its version number.  Moreover, for each datatable within Sqltable,  we store in DataTableVersion its schema, a default value map, and two maps from column oid to id and from id to oid.

To do an SQL query such as updating a tuple, a transaction first uses the index on Sqltable to find the tupleslot of the tuple (note that we update the index when migrating tupeslots). We can get the raw block from the top 44 bits of the tupleslot, and the raw block has reference to the datatable of the tupleslot.   Thus we know the version number of the datatable the tuple is current stored in, call this the *storage_version*.

Each SQL query also comes with an argument, version_number, which is the schema version of the Sqltable that should be seen by the current transaction. Let us call these versions *intended_version*. 

To migrate a tuple with primary key *K* from an older datatable to a newer datatable, we first logically delete the tuple from the older datatable with the current transaction's timestamp, then insert the tuple to the newer datatable. Note that the old tupleslot for *K* has been deleted and a new tupleslot was created. We will update the index by logically deleting *K* then inserting *K* with the new tupleslot.

Note that *storage_version* might be smaller than *intended_version*, which occurs if a transaction accesses a tuple of a newer schema version, but the tuple has not yet been migrated over from old datatables. Different Sqltable operations deal with this special case of  *storage_version < intended_version* differently, as we will  describe below.

#### Sqltable::insert 

There is no *storage_version* in this case. We directly insert to the table corresponding to *intended_version*.

#### Sqltable::delete

Whether *storage_version < intended_version* or *storage_version = intended_version*, we always logically delete the tuple from the datatable it is currently stored in.

#### Sqltable::update

If *storage_version < intended_version* we will read the tuple from the old datatable, do tuple transformation to new schema and update the tuple, then logically delete it from the old datatable  (corresponding to 
*storage_version*) and then insert it to the new datatable (corresponding to *intended_version*).  We will return the new tupleslot to the execution layer, which will update the index as described above.

#### Sqltable::select

Since we don't do migration on reads for now, we will not migrate tuples even in the case of *storage_version < intended_version*. We will read the tuple, transform it to the new schema, select its columns and return.

#### Sqltable::scan

Note that each tuple might physically exist in multiple datatables under a single Sqltable. However, for each transaction, only one copy of each tuple is visible across all datatables (we guarantee this by always logically deleting a tuple before migration).

Therefore, scan works by scanning all datatables in order. Recall that tables_ is an ordered map, so we can simply scan from tables_.begin() to tables_.end(). We will scan each datatable fully, can jump to the next datatable when we reach the end. It is possible that we need to revise the order to scan from tables_.end() to tables_.begin(), because in the special case that a single transaction interleaves scanning with updating, we might scan the same tuple twice (it is first scanned, then migrated to the end of tables_, then scanned again).

#### Default Values

Default values of columns are abstract expressions that can be evaluated. Since the schema of each datatable is fixed, for each datatable we store a map in its DatatableVersion struct specifying the default values of all columns in its schema. The map is from column id to abstract expression. We can use this map for tuple transformation.

#### Tuple Transformation

We need to translate column_ids in the storage version to column_ids of the intended version. For any column present in the storage version but not present in the intended version we set the column_id to IGNORE_COLUMN_ID, and within DataTable we will skip over any columns with IGNORE_COLUMN_ID . For any column present in the intended version but not present in the storage version, we fill in default values for columns that were not present in the storage version.

### Catalog
#### Updating the schema
We implemented a DDL executor for the ALTER TABLE command that updates the catalog when a table is altered. Scehma information for each table (pointer to the schema) is stored in a column in the catalog tables (`classes_` to be more specific). So after a new schema is generated by evaluating the ALTER TABLE commands, we update the schema pointer column for the table being altered. 
We also add a `layout_version` column along side the schema pointer column for each table. 
Since the catalog tables are transactional, they disallow write-to-write conflict from MVCC. As a result, concurrent schema updates are caught here as one of the updating transactional will abort when trying to modify the catalog table. 

For multiple schema pointers, we created another catalog table called `pg_schema` that stores the schema pointer,the table oid and the layout version for that schema. So when an update schema query is executed, the pg_schema will add one more entry, and it will be responsible for freeing the schema pointer afterwards. 
This design allows the schema pointer to live beyond the scope of MVCC (so the version chains do not need to free the schema memory). In addition, this design will allow us to query a specific layout later on by consulting the `pg_schema` table, such as selecting from different layout versions at the SQL query. 

#### Getting the layout version
For each transaction that accesses the SqlTable, it needs to know which layout version it should be looking at. We leverage on the transactional tables in the catalog to translate the correct visible layout version for an in-flight transaction. Transaction will read the catalog tables to retrieve the most recent visible layout version, and use it to access the SqlTable.

### ALTERTABLE command (parser, binder, optimizer, planner, and execution)
ALTER TABLE commands are parsed, binded and optimized just like other commands to generate a plan node tree. As for now, we are only supporting schema updates that do not require scanning of the underlying SqlTable (such as adding columns, dropping columns etc). Such commands only require updating the catalog tables, and the SqlTable's meta data. So a DDL Executor is implemented to execute the generated plan.


## Design Rationale

#### Map or vector of datatables? 

Within Sqltable, we can either use a ordered concurrent map of version number to datatable, or use a vector of datatables (index i of vector corresponds to datatable with version number i) augmented with locks.  

We decided to use a fixed-size vector, where the datatable with version i is stored in the i-th position of the vector. We use a MAX_NUM_VERSIONS as the size of the vector, which is the max allowed number of versions for a single datatable.

We use a fixed-size vector because of low synchronization overhead: there is no need for locking the vector since different versions access different indices. Also, we need not make a special case for tables with a single version, because accessing an index in the vector is as fast as reading a pointer. Using a vector is thus much faster than a concurrent map.

One potential issue with fixed-size vector is dealing with datatables with more than MAX_NUM_VERSIONS. This should be extremely rare if we set MAX_NUM_VERSIONS, since schema changes generally don't happen too often on a datatable. When we support GC of stale versions, we can implement recycling segments of the vector with stale versions, and adding offset to allow using this vector in a cyclic fashion,

#### When to perform background migration and GC of old versions?

When we are sure that no transactions will ever access the schema version represented by an old datatable(inferring from low_watermark of transaction timestamps), we can safely GC the outdated datatable. However, since we do lazy migration, if most tuples in a datatable is not updated after a schema change, they will still be in the outdated datatable, and we can read and access them from the outdated datatable. In this case, it might be quite expensive to GC the outdated datatable, since we need to first migrate all its tuples to newer datatables. It seems that doing background migrations conservatively is good for read-heavy workloads, and doing background migrations aggresively might be good for update-heavy workloads interleaved with schema changes.  

Our current decision is to use a threshold to decide whether to do background migration: once the amount of tuples in an outdated datatable drops below the threshold, we start a background thread to migrate its tuples and GC the datatable when we're done.

Currently background migration has not been implemented.

#### Should we do migration on reads?

This is related to the above decision on GC. Currently, we only migrate tuples on Sqltable::update, if the tuple belongs to a newer datatable. Another policy is to also do migration on reads: after reading a tuple that belongs to a newer datatable (in Sqltable::select or Sqltable::scan), we migrate it to the new datatable before reading it. The benefit of migration on reads is: if we read a tuple from an outdated table multiple times,  transforming the tuple to its newest schema every time can be costly. With migration on reads, we need to do tuple transformation only once. Migration on reads  works well with aggresive migration. However, migration on reads might need to take write locks to do the migration, which can considerably slow down throughput of reads.

Our current decision is to not do migration on reads, since it might affect throughput. We will revisit this decision when we finish implementing GC and start benchmarking throughput.

## Testing Plan

First, we wrote single-threaded unit tests in sql_table_test.cpp that tests schema changes. We test adding and dropping multiple columns in a single schema change, combining multiple schema changes with various SQL table operations (insert/delete/select/scan/update). We also added multithreaded versions of the same tests to test correctness under concurrent threads.
We also added unit tests to test the correctness of the changes we made to the binder, execution, optimizer, and parser layers. 

We also wrote a update_schema_benchmark that performs consecutive update_schemas along with concurrent normal Sqltable operations, both to test the correctness of our changes to the sqltable layer, and to test the throughput with schema changes (under different workloads, read-write-delete, insert-heavy, etc.).

Finally, as a stretch goal, we can integrate the Pantha Rei Schema Evolution Benchmark, which contains over 4.5 years of schema changes in Wikipedia’s history, and use this real world benchmark to test the throughput and memory usage of our non-blocking schema change implementation.

## Trade-offs and Potential Problems

One potential problem is we are unsure how adding versions and multiple datatables will influence logging and recovery. We can revisit this after the recovery group merges their changes.

## Future Work
1. Can support more types of ALTER TABLE SQL commands such as change type and change default value. Currently we only support add and drop column. 
2. Current we have separate classes for each type of ALTER TABLE, which might need to be compacted if there are a dozen types of ALTER TABLE.
3. Support background migration of tuples, and GC of unused schema versions.
4. For scanning tuples in a table to check if a type change is allowed, we can use TPL.
5. Support unsafe schema changes, which includes updating type, nullable, or default value status of columns.
6. One optimization is to precompile the tuple transformations. The code for different tuple transformations are mostly similar, except that the types of columns are different. Therefore, we can add code to the codegen layer to precompile the tuple transformation logic for different column types. This may speed up tuple transformations.
7. In the future, we might want to get rid of the sqltable layer all together, so schema change logic should be moved to the execution layer.

