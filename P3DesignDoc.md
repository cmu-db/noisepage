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

### Constraint Creation during Table Creation


## Design Rationale
>Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.

## Testing Plan
>How should the component be tested?

## Trade-offs and Potential Problems
>Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).

## Future Work
>Write down future work to fix known problems or otherwise improve the component.
