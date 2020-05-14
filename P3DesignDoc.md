# Database Constraints
Authors: Yingjing Lu, Wuwen Wang, Yi Zhou

## Overview

Constraints are important part of the relational database to ensure data integrity and table entity relation. Common constraints in SQL style DB includes: NOT NULL, UNIQUE, FOREIGN KEY, and CHECK. Those constraints can either be defined during the database and table creation column to be applied into table column, or to be altered afterwards using ALTER statement. Depending on the implementation, those constraints can be applied in an online fashion or offline. In this project we focus mainly to implement constraint checking support for PRIMARY KEY, UNIQUE and FOREIGN KEY. Our implementation follows the behavior of the Postgres SQL 12. 

## Scope
To align with each stage of the development, we formulate our design goal in terms of 75%, 100% and 125%. Which correlates to basic functionality implementation, complete working implementation, and optional advanced feature implementation.

##### 75% Goal
- [x] Implement support of basic constraint PRIMARY KEY and UNIQUE definition during the create table process
- [x] Implement constraint checking procedure during data Insertion process
- [x] Create associated unit test to ensure the correctness of the implementation

##### 100% Goal
- [x] Implement FOREIGN KEY definition during the create table process
- [x] Implement FOREIGN KEY checking during data insertion, Update process
- [x] Create associated unit test to ensure the correctness of the implementation

##### 125% Goal
* Create support of ALTER statement for constraint optimization during parsing and planning
* Create support of alter constraint for existing table in an online fashion
* Create associated unit tests to check for correctness of the implementation

## Architectural Design

### Constraint Storage Definition
>catalog/postgres/pg_constraint.h

This is the definition of what information of what a constraint information should be stored. The constraints are stored in forms of table and the row definition is also defined here.

```C++
constexpr col_oid_t CONOID_COL_OID = col_oid_t(1);        
                    // INTEGER (pkey)
constexpr col_oid_t CONNAME_COL_OID = col_oid_t(2);       
                    // VARCHAR - name of the constraint
constexpr col_oid_t CONNAMESPACE_COL_OID = col_oid_t(3);  
                    // INTEGER (fkey: pg_namespace) - namespace of the constraint
constexpr col_oid_t CONTYPE_COL_OID = col_oid_t(4);  
                    // CHAR - type of the constraint, expressed in char defined below
constexpr col_oid_t CONDEFERRABLE_COL_OID = col_oid_t(5);  
                    // BOOLEAN - is the constraint deferrable
constexpr col_oid_t CONDEFERRED_COL_OID = col_oid_t(6);    
                    // BOOLEAN - has the constraint deferred by default?
constexpr col_oid_t CONVALIDATED_COL_OID = col_oid_t(7);  
                    // BOOLEAN - has the constraint been validated? currently can only be false for FK
constexpr col_oid_t CONRELID_COL_OID = col_oid_t(8);  
                    // INTEGER (fkey: pg_class) - table oid of the table this constraint is on
constexpr col_oid_t CONINDID_COL_OID = col_oid_t(9);  
                    // INTEGER (fkey: pg_class) - index oid supporting this constraint, if it's a unique, primary key,
                    // foreign key, or exclusion constraint; else 0
constexpr col_oid_t CONPARENTID_COL_OID = col_oid_t(10);  
                    // INTEGER (fkey: pg_constraint) - The corresponding constraint in the parent partitioned table, if
                    // this is a constraint in a partition; else 0
constexpr col_oid_t CONFRELID_COL_OID = col_oid_t(11);  
                    // INTEGER - (fkey: pg_class) reference table oid of the fk constraint, 0 for other constraints
constexpr col_oid_t CONFUPDTYPE_COL_OID = col_oid_t(12);  
                    // CHAR - type of the cascade action when updating reference table row
constexpr col_oid_t CONFDELTYPE_COL_OID = col_oid_t(13);  
                    // CHAR - type of the cascade action when deleting reference table row
constexpr col_oid_t CONFMATCHTYPE_COL_OID = col_oid_t(14);  
                    // CHAR - type of matching type for fk
constexpr col_oid_t CONISLOCAL_COL_OID = col_oid_t(15);  
                    // BOOLEAN - This constraint is defined locally for the relation. Note that a constraint can be
                    // locally defined and inherited simultaneously.
constexpr col_oid_t CONINHCOUNT_COL_OID = col_oid_t(16);  
                    // INTEGER - The number of direct inheritance ancestors this constraint has. A constraint with a
                    // nonzero number of ancestors cannot be dropped nor renamed.
constexpr col_oid_t CONNOINHERIT_COL_OID = col_oid_t(17);  
                    // BOOLEAN - This constraint is defined locally for the relation. It is a non-inheritable constraint.
constexpr col_oid_t CONKEY_COL_OID = col_oid_t(18);  
                    // VARCHAR - [column_oid_t] space separated column id for affected column. src_col for fk
constexpr col_oid_t CONFKEY_COL_OID = col_oid_t(19);  
                    // VARCHAR - [column_oid_t] space separated column id for affected column ref_col for fk, empty for other
constexpr col_oid_t CONPFEQOP_COL_OID = col_oid_t(20);  
                    // VARCHAR - [] space separated op id If a foreign key, list of the equality operators for PK = FK comparisons
constexpr col_oid_t CONPPEQOP_COL_OID = col_oid_t(21);  
                    // VARCHAR - [] space separated op id If a foreign key, list of the equality operators for PK = PK comparisons
constexpr col_oid_t CONFFEQOP_COL_OID = col_oid_t(22);  
                    // VARCHAR - [] space separated op id 	If a foreign key, list
                    // of the equality operators for FK = FK comparisons
constexpr col_oid_t CONEXCLOP_COL_OID = col_oid_t(23);  
                    // VARCHAR - [] space separated op id If an exclusion constraint, list of the per-column exclusion operators, 
                    // NOTE: if FK constraint the source column index_oid is stored here as string
constexpr col_oid_t CONBIN_COL_OID = col_oid_t(24);  
                    // BIGINT - If a check constraint, an internal representation of the expression
                    // SET to CONBIN_INVALID_PTR for other constraint types
```

Supporting Constraints:
- [] CHECK (CONTYPE_COL_OID = 'c') - The CHECK constraint to verify the column data is in a specific set.
- [x] FOREIGN KEY (CONTYPE_COL_OID = 'f') - Associate the data of the column in one table with data in another
- [x] PRIMARY KEY (CONTYPE_COL_OID = 'p') - The promary key constraint of the table, implies UNIQUE and NOT NULL
- [x] UNIQUE (CONTYPE_COL_OID = 'u') - Data in the column has to be unique
- [] TRIGGER (CONTYPE_COL_OID = 't') - Trigger constraint to invoke trigger during insert
- [] EXCLUSION (CONTYPE_COL_OID = 'e') - Exclusion constraint to exclude some set of data in this column
- [] NOT NULL (CONTYPE_COL_OID = 'n') - Not NULL Constraint: a single column constraint

Supporting FKActionType:
- [] NOACT = 'a',        // no action
- [] RESTRICTION = 'r',  // restrict
- [x] CASCADE = 'c',      // cascade
- [] SETNULL = 'n',      // set null
- [] SETDEFAULT = 'd',   // set default
- [] SETINVALID = 'i'    // set invalid

Supporting FKMatchType:
- [x] FULL = 'f',     // fully match when compare
- [] PARTIAL = 'p',  // partially match when compare
- [] SIMPLE = 's',   // simple match when compare

## Design Rationale
The primary focus is to align with what other postgres APIs such as ph_index and pg_namespace are currently formulated. We want to make sure that their creation, check and modification are positioned under the same files with similar APIs to ensure consistency. At the same time we still want a separate pg_constratin class module to encapsulate the specific constratin checking logic to isolate that from the other modules but can be accessed from the same API call style.

* Insert builtin call for constraint verification during the Codegen phase at insert/delete/update_translator
* Insert constraint creation during table creation at execution phase at execution/sql/ddl_executor
* Insert constraint checking and verification following the TableInsert pipeline:
execution/sql/storage_interface -> catalog/catalog_accessor -> catalog/database_catalog

## Testing Plan
> tests/catalog/constraint_statement_test.cpp
Runtime testing is the basic and most straight-forward way to test constraint. We will have automated terminal loading and enter the SQL statement for creation, INSERTION and DELETE and check if the constraints are enfoced during the process. We will also check for resistence on the conflicting statements such as FOREIGN KEY on a non-key parent.

## Trade-offs and Potential Problems
The original version normalize the pg_constraint table and allow each constraint (foreign key, check, exclusion) to be managed in separate table with different class API modules. However, per required during review we switched back to follow the postgres standard. This let pg_constraint to hold all information for all constraint, but follows the pg_constraint spec.

## Future Work
This would be a TODO list and will keep updating:
- [] Index created for constraint for foreign key reference table is not automatically propagated. This is another group's project
- [] More supported action for foreign key verification
- [] other constraint support such as CHECK, EXCLUSION, NOT NULL, TRIGGER, etc
- [] pg_operator for check, FK, and exclusion to further comply with postgres design
- [] some deatiled todo are embedded in code with todo assignee named "pg_constraint"
