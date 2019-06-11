# Catalog Design Documention

TODO(John): Port this document over to the new format.

## File Hierarchy
```
src/
    catalog/
        postgres/
            builder.cpp
        catalog.cpp
        catalog_accessor.cpp
        database_catalog.cpp
    include/
        catalog/
            postgres/
                builder.h
                pg_attribute.h
                pg_class.h
                pg_constraint.h
                pg_database.h
                pg_index.h
                pg_namespace.h
                pg_type.h
            catalog.h
            catalog_accessor.h
            catalog_defs.h
            catalog_schema.dot
            database_catalog.h
            index_schema.h
            README.md
            schema.h
```
## Classes

### Catalog
Main entry point for the catalog.  Only directly handles bootstrap/debootstrap entry and direct manipulation of entire databases.

### CatalogAccessor
Stateful wrapper around the catalog to make user code easier to write without having to worry about the internals of the catalog.

### DatabaseCatalog
Primary entry point for most DDL operations.

### Schema
Stateful wrapper that allows a table schema to be described programmatically and then applied in a single call.  User modifications/instantiations will lack OIDs (or hide them) and the call to the catalog will resolve what the OIDs are.

### Schema::Column
Subordinate class to `Schema` that provides the detailed definitions of each column in a table.

### IndexKeySchema
Analog to `Schema` but idempotent (except for OIDs) after creation.

### IndexKeySchema::Column
Analog to `Schema::Column` but idempotent (except for OIDs) after creation.

### [Table]Entry
Stateful wrapper around a projected row for each of the implemented _PostgreSQL_ catalog tables that provides strongly typed accessor methods for each column to allow compile-time type checking.

### postgres::Builder
A static helper class that exists to enable certain aspects required for bootstrapping such as self-assigning OIDs and other public API circumventing activities.

## Design

### High-Level Design
The general goal of the catalog is to support storing and accessing all of the metadata that the binder, optimizer, and execution engine need to execute both DML and DDL queries.  While our goal is to support _PostgreSQL_ tools, we wanted to decouple our internal API from the table storage to provide flexibility moving forward in order to minimize inherited technical debt from _PostgreSQL_ and ensure our internal API can remain stable even as the catalog table schemas evolve.

Additionally, we wanted to lower the barrier to entry for people writing code needs to interact with the catalog.  Specifically, we wanted to ensure that they had a simple, single interface (`CatalogAccessor`) that they could which allows them to use the catalog without having to know or reason about the backend implementation.

### Significant Differences from _PostgreSQL_'s Catalog

#### Pointer Columns
In order to make OID to object resolution efficient and transactional, we have added columns that store pointers to C++ objects to tables that correspond to these objects (i.e. `pg_database` and `pg_class`) as well as pointers to cached materializations of schemas and expressions.

#### Merging of `pg_attribute` and `pg_attrdef`
_PostgreSQL_ uses a separate table for default values in order to ensure default value changes do not block concurrent transactions because of the exclusive locks it needs to acquire.  Since our system does not have this semantic, we have merged the tables to enable a simpler interface.

#### Overloading of `pg_attrdef` Fields
We overload the `adbin` and `adsrc` fields of the `pg_attrdef` table to store the expressions for constructing index key columns.  While this may be semantically awkward at the table level, we can do this transparently to the user in the API and reduce backend complexity by not having to implement significantly different logic for handling index schemas compared to table schemas.

### Table Schema
The file `catalog_schema.dot` in the same directory as this README provides a description of the table schema for the catalog in _Graphviz_ format.  To turn it into a graphic you need the _Graphviz_ program.  With that the commant `dot -Tpdf -oschema.pdf catalog_schema.dot` will compile the description into a PDF visualization.
