# Non-Blocking Alter Table Support (SqlTable)

## Overview
The overall goal of this project is to implement lazy, non-blocking schema changes. The current storage layer does not support any schema changes. The first goal of this project is to support schema change operations including add column, drop column, and default value changes. However, the overall goal would be to carry this out in a non-blocking fashion, for which we decided to go with a lazy evaluation approach. This means that any schema change would not migrate existing tuples to the new schema until information that only exists in the new schema is modified.

## Scope
Almost all of the work will be localized to the SqlTable object as that is the access point from the execution layer to the storage layer. Also our design will not affect the underlying structures of the tuples or the DataTables and after this change they should still be able to provide their functionality without having to go through the SqlTable. Within the SqlTable we will be changing the following modules. Below we will refer to expected version as the version of the tuple the user expects to see and actual version as the version the tuple is currently in. These two versions will differ when a user has updated schema, but due to the lazy evaluation the tuple hasn’t been transformed into the latest version.

### SqlTable::Insert
- Insert will now take in a schema version number that indicates the version of the tuple being inserted.
- In this case the actual and expected versions will be the same as an insert will always put a tuple in the version that is passed in.

### SqlTable::Update
- Update will be required to take in a schema version number that indicates the expected schema version number of the tuple being updated
- In this case the actual version number will differ from the expected version in cases where the tuple being updated has not been shifted to the expected version.
- If the update modifies columns that are not in the actual version then the tuple will be shifted to the expected version before applying the update.

### SqlTable::Scan
- Scan will be required to take in a schema version number that indicates the schema version number that the current transaction sees.

### SqlTable::Delete
- Delete will be required to take in the schema version number that indicates the expected schema version number of the tuple being deleted

### Catalog
- The catalog will keep track of the visible schema versions for each transactions based on its timestamp. This schema version is then passed on to the SqlTable layer.

### DataTable/Projected Row
- DataTable::SelectIntoBuffer iterates across a projected_row/column’s column ids and fills in the data for each column

    - For this operation we use the column_id VERSION_POINTER_COLUMN_ID as a sentinel id to represent a column that the DataTable should skip over and not fill in. We will go into detail as to why this happens within the architectural design below.


## Architectural Design
The design of this project will center around the modifications to SqlTable. The design for other components in the storage layer will remain unchanged. On a schema change the SqlTable will create a new DataTable that will store all the tuples inserted from that point on into the new version. To be lazy it will not modify already existing tuples to transform them into the latest version.  In order to support this lazy schema change we need two functionalities: maintaining tuples in multiple different schema versions and providing methods of transforming them into the desired version.

### Multi-versioning
To address the multi-versioning the SqlTable will maintain a map from the schema_version_number to a DataTable. There will be one DataTable for each schema version. The functionality for accessing tuples is already present in DataTable and this way SqlTable will only need to manage the two functionalities we described above. The rest of the functionalities will be handled by the already existing DataTable implementation. Furthermore, each block will maintain metadata of which version all of the tuples within the block belong to. Since each DataTable can only be a single version, a block cannot contains tuples from multiple versions.  Below is a description of the multiversion design for each of the SqlTable operations we are modifying and we will refer to expected version as the version of the tuple the user expects to see and actual version as the version the tuple is currently in.

#### SqlTable::Insert
Insert will always insert the passed in tuple into the DataTable of the schema version number passed in

#### SqlTable::Update
- Update has three cases
    1. The expected schema version matches the actual schema version
        - The update will happen in place on the DataTable of the actual schema version
    2. The expected schema version doesn’t match the actual schema version but the update doesn’t touch any columns not in the actual schema version
        - The update will happen in place on the DataTable of the actual schema version
    3. The expected schema version doesn’t match the actual schema version and the update touches not in the actual schema version. The following steps occur
        - Retrieve the tuple from the actual version DataTable
        - Transform the tuple to the expected version
        - Delete the tuple from the actual version DataTable
        - Insert the tuple into the expected version DataTable

#### SqlTable::Scan
- SqlTable will maintain its own slot iterator which will be used to iterate across all the schema version. The iterator interface exposed to the user will not change
- The iterator for Scan must always begin on the latest version
    - In cases where the user is interchanging scan call and update calls, if the scan iterator were to start on an older version then it is possible for a tuple retrieved from the scan to be updated, which could move it into the latest schema version
    - This would mean that when the iterator gets to the latest schema version it will read all the tuples in the DataTable for that version and this would cause that tuple to have been read twice within the scan.

#### SqlTable::Select
- There are two cases
    1. Expected schema version matches actual schema version
        - The tuple is directly selected from the DataTable for that schema version
    2. Expected schema version differs from the actual schema version
        - The tuple is selected from the DataTable for that schema version and during this process it is transformed to the expected schema version
        - This transformed tuple is returned

#### SqlTable::Delete
- Delete will directly delete the tuple from the DataTable of the actual version of that tuple

#### SqlTable::UpdateSchema
- This function is the access point through which users can update the schema by passing in a new schema object
- The SqlTable will construct a new DataTable to maintain all of the tuples inserted for this version.
- To be lazy, none of the already existing tuples will be modified in this call.

### Transformation
In the current interface the user can only retrieve data from a SqlTable through a ProjectedRow or a ProjectedColumn, we will refer to both as Projection in this section for simplicity. The user passes in a Projection which is filled by the storage layer. The Projection passed in by the user will be in the expected version but the actual version of the data could be different so we need to provide a way of transforming between versions. To do this we modify the header of the Projection.

The header contains metadata regarding column_ids and column_offsets which is used by the DataTable to populate the Projection. Furthermore the column_ids can be different between schema versions. Before passing the Projection to the DataTable of the actual version we translate the column_ids that are in the expected version to column_ids of the actual version. Then for any column that is not present in the actual version we set the column_id to a sentinel value (VERSION_POINTER_COLUMN_ID, as no column in the Projection should have that id). We pass this modified Projection to the DataTable which populates it, skipping over any columns with the id set to the sentinel value. Then the we reset the Projection header to the original header and fill in any default values for column that were not present in the actual version. This way we avoid having to copy data from one version to another and it is filled in only once.

## Design Rationale
In order to support lazy schema changes we will need to maintain tuples in multiple different schema versions and provide methods of transforming them into the desired version.

### Backend for different schema versions
The initial decision for storing schema versions within an SQL table was whether to back it by a vector or map.  While we appreciated the simplicity and probable performance benefits of a vector, we ultimately decided to go with a map because it did not force our versioning to start at 0.  While this constraint does not seem significant at first, we realized that if the database is restored from a checkpoint we should restore the schema version number (since it may be exposed to the user or tracked by a hot-backup) rather than reinitialize the versions to 0.

The second decision for our backend was whether we should protect the underlying data structure with latches or use a latch-free structure.  We ultimately decided to use a latch-free data structure because we decided it would be difficult to reason about every possible point the latch would be needed (essentially any version check) without wrapping the map in another abstraction level.  Additionally, we had serious concerns about introducing 4 to 10 latch operations on the path of every single SQL action in every single transaction and that would guarantee large numbers of cache invalidations.  We therefore decided to go with the ConcurrentMap implementation in the database (wrapper of tbb::concurrent_hash_map) which supports the common case of concurrent insertions and lookups.  However, this creates future difficulties for supporting compaction/deletion of obsolete schema versions because erasures are unsafe.  Unfortunately, we are not aware of any candidate hash map implementation that supports safe insertions, deletions, and lookups without utilizing latches.

### Transforming old versions into the expected version
We recognized two possible ways to transform the data stored under old schemas into the expected schema for a transaction:  (1) attribute-wise copying of the data from an old ProjectedRow to a new one and (2) rewriting the header on the new ProjectedRow (provided by the caller) to be readable by an older data table.  We initially implemented (1) because it was far simpler logic.  However, when we benchmarked the implementation for cross-version selects we observed a significant performance penalty (factor of 10x).  We therefore have switched to (2) and have reduced the penalty to a factor of 5x.  We are still working on improving this even further.

## Testing Plan
Our current testing plan is to implement two new test suites that test both the sequential correctness and concurrent robustness of the implementation.  The sequential test suite focuses on ensuring that known edge cases are handled correctly.  We focus on a sequential test for these situations because we can more tightly control the ordering of actions.  We are also implementing a concurrent test suite which will ensure that performs a mini-stress test that tries to verify that rare, but possible, race conditions are likely to be detected.  For this we focus on straining access to the versioning scheme by ensuring we are doing concurrent inserts and reads on the hash map.

In addition to formal tests, we are also benchmarking our implementation as we go to ensure we measure and understand the performance impacts of our changes.  Specifically, we are focusing on performance impact across a range of simulated workloads (selects, inserts, updates, and a mix) and in two general situations:  a single schema version and multiple versions.  The goal here is to ensure we can quantify our impact against the current single-versioned implementation as well as quantify the performance degradation for on-the-fly schema manipulation of old data that has not migrated to the new schema version.

## Trade-offs and Potential Problems
**Trade-off:** TBB Concurrent Unordered Map for storing DataTableVersion objects.  This decision gives a simple and easy to integrate solution for supporting concurrent insertions (new schemas) and lookups (all DML transactions) on the data tables.  However, this will limit options when we start to implement compaction of obsolete schema versions because the data structure does not support concurrent erasures.  This likely means we will have to take a heavy-weight approach for compaction such as duplicating the structure without the data to be erased and then use an unlink-delete staged method similar to how the GC already works on undo records.

**Trade-off:**  Our decision to manually mangle the headers for ProjectedRow and ProjectedColumn greatly increases the code complexity (manual recreation of the headers) in order to significantly improve performance for reading data across schema versions.  Specifically, we avoid an unnecessary allocation and deallocation for temporary projections by allowing old schema versions to write directly into the final projection.

## Future Work
### Pending tasks
#### Default values
- Populate the default values into the ProjectRow during Select and Scan operations.
- Handle changes to the default values. Should they be considered as a schema change or Catalog maintains the default values that can be queried by the SqlTable?

### Stretch goals
- Rolling back schema change transactions.
- Implementing a Compactor to remove DataTables of older versions that don’t contain any tuples.
- Serializing transactions with unsafe/conflicting schema changes by using a central commit latch, allowing only one transaction to commit at a time. Rollback if the validation checks fail.
