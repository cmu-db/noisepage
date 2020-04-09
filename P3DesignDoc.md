# Database Constraints

## Overview

Constraints are important part of the relational database to ensure data integrity and table entity relation. Common constraints in SQL style DB includes: NOT NULL, UNIQUE, FOREIGN KEY, and CHECK. Those constraints can either be defined during the database and table creation column to be applied into table column, or to be altered afterwards using ALTER statement. Depending on the implementation, those constraints can be applied in an online fashion or offline. 

## Scope
To align with each stage of the development, we formulate our design goal in terms of 75%, 100% and 125%. Which correlates to basic functionality implementation, complete working implementation, and optional advanced feature implementation.

##### 75% Goal
* Implement support of basic constraint NOT NULL and UNIQUE definition during the create table process
* Implement constraint checking procedure during data Insertion process
* Create associated unit test to ensure the correctness of the implementation

##### 100% Goal
* (Building on 75% goal)
* Implement FOREIGN KEY definition during the create table process
* Implement FOREIGN KEY checking during data insertion process
* Create associated unit test to ensure the correctness of the implementation

##### 125% Goal
* Create support of ALTER statement for constraint optimization during parsing and planning
* Create support of alter constraint for existing table in an online fashion
* Create associated unit tests to check for correctness of the implementation

## Architectural Design

### Constraint Storage Definition
>catalog/postgres/pg_constraint.h

This is the definition of what information of what a constraint information should be stored. The constraints are stored in forms of table and the row definition is also defined here.

```C++
col_oid_t CONOID_COL_OID;         // INTEGER (pkey) - pkey for a constraint
col_oid_t CONNAME_COL_OID;        // VARCHAR - name of the constraint
col_oid_t CONNAMESPACE_COL_OID;   // INTEGER (fkey: pg_namespace) - namespace of the constraint
col_oid_t CONTYPE_COL_OID;        // CHAR - type of the constraint
col_oid_t CONDEFERRABLE_COL_OID;  // BOOLEAN - is the constraint deferrable 
col_oid_t CONDEFERRED_COL_OID;    // BOOLEAN - has the constraint deferred by default
col_oid_t CONVALIDATED_COL_OID;   // BOOLEAN - has the constraint validated
col_oid_t CONRELID_COL_OID;       // INTEGER (fkey: pg_class) - the relation id of the constraint
col_oid_t CONINDID_COL_OID;       // INTEGER (fkey: pg_class) - the index supporting this constraint, if it is UNIQUEm FK or EXCLUSION, 0 otherwise
col_oid_t CONFRELID_COL_OID;      // INTEGER (fkey: pg_class) - if a foreign key, the reference table oid, else 0
col_oid_t CONBIN_COL_OID;         // BIGINT (assumes 64-bit pointers) - if a check constraint, the internal representation of expression of check
col_oid_t CONSRC_COL_OID;         // VARCHAR - The identifier of the columns that the constraint applies to
```

Supporting Constraints:
* CHECK (CONTYPE_COL_OID = 'c') - The CHECK constraint to verify the column data is in a specific set.
* FOREIGN KEY (CONTYPE_COL_OID = 'f') - Associate the data of the column in one table with data in another
* PROMARY KEY (CONTYPE_COL_OID = 'p') - The promary key constraint of the table, implies UNIQUE and NOT NULL
* UNIQUE (CONTYPE_COL_OID = 'u') - Data in the column has to be unique
* TRIGGER (CONTYPE_COL_OID = 't') - Trigger constraint to invoke trigger during insert
* EXCLUSION (CONTYPE_COL_OID = 'e') - Exclusion constraint to exclude some set of data in this column

### Constraint Creation during Table creation
>catalog/catalog_accessor.h
>catalog/database_catalog.h
>execution/sql/ddlexecutor.h

Here we added API for CreateConstraints and added that into the ddlexecutor when creating table. This is called in a similar style as create_index when creating table to create constraint and record that in pg_constraint table.

### Constraint Checking during Insertion
>storage/data_table.h

During the insertion, and deletion, we want to add code to the existing API to enforce the constraints. 

During Insertion, we check for:
* UNIQUE Constraints
* NOT NULL constraints on the projected vector
* FOREIGN KEY constraints to ensure their foreign key matches

During Update and Deletion we check for:
* FOREIGN KEY cascade if necessary

### Constraint Class APIs for check
>catalog/pg_constraint.h
**Constraint Modifier** - modify the constraints by their id
* Constraint Getter - get the constraints by oid, table and by type
* Constraint Setter/Updater - set and update according to the table id and constraint id
* Constraint Deletion - delete constraints by table_oid/constraint_oid

**Constraint Enforcer** - check and enfoce constraint during operation
* Constraint checker for NULL on different types of data and different def of NULL
* Constraint checker for FOREIGN KEY/UNIQUE: comparator for equality among different data types
* Constraint checker for PRIMARY KEY: implied check for their UNIQUE and NOT NULL property
* Constraint CASCAD: going recursively from child constraint to parent table and modify CASCADE

## Design Rationale
The primary focus is to align with what other postgres APIs such as ph_index and pg_namespace are currently formulated. We want to make sure that their creation, check and modification are positioned under the same files with similar APIs to ensure consistency. At the same time we still want a separate pg_constratin class module to encapsulate the specific constratin checking logic to isolate that from the other modules but can be accessed from the same API call style.



## Testing Plan
**Runtime testing**
Runtime testing is the basic and most straight-forward way to test constraint. We will have automated terminal loading and enter the SQL statement for creation, INSERTION and DELETE and check if the constraints are enfoced during the process. We will also check for resistence on the conflicting statements such as FOREIGN KEY on a non-key parent.

**Unit Testing**
unittesting is meant to ensure that each of our module are correctly implement and behaves correctly under different edge cases and concurrency. (More to add as development moves to FOREIGN KEY)

**Performance Testing**
We also want to create benchmark test to look for potential bottleneck or overhead in our implementation that migh hurdle the overall performance and try to address in case the constraint is in a hot area.

## Trade-offs and Potential Problems
The original (abandoned) plan was to create a completely separate hash table structure and manager for the constraint instead of utilizing a table to manage constraint. This could further isolate constraint API and utilization from other parts of the code and make them more modular, we choose not to use this for two reason: 
* It adds additional overhead for checkpoint and concurrency, as additional component are frozen for checkpoint and update and might create additional bottleneck for optimization. 
* Secondly, the constraints are not aligned with the format of other components which makes harder to manage through a consistent API format for where and how APIs are located and called.

## Future Work
This would be a TODO list and will keep updating:
* Create the above mentioned Constraint class for all APIs
* Implement NOT NULL Constraint
* Implement the FOREIGN KEY Constraint
* Implement FOREIGN KEY to support CASCADE
* Write the runtime test queries and expected result
* Write the unittest for edge cases on both constraints
* Implement support for planning for ALTER constraint statement
* Implement online ALTER statement on constraints
* Write the benchmark test
