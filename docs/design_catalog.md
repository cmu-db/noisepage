# Design Docs: Catalog

This provides an overview of the code structure and APIs for the catalog.

Each table in the catalog has two classes, `Handle` and `Entry`.

- `Handle`, e.g., `NamespaceHandle`: The handle provides methods for the overall table. These included:
  - Create (used during initialization)
  - Doing lookups, returning an `Entry` (see below)
  - Delete
- `Entry`, e.g., `NameSpaceEntry`: An entry is a single row result from a lookup with methods to access the columns by name.
  - The contents of an entry do not change once it has been created.

The `Catalog` class contains higher level methods:

- creating and deleting databases
- creating and deleting tables
- obtaining handles for the catalogs.

The structure of the catalog tables is generally taken from Postgres. Detailed information is present in the Postgres documentation. Briefly,

- A `DatabaseHandle` provides access to pg_database. pg_database is global, and records databases.
- A `NamespaceHandle` provides access to pg_namespace. There is a pg_namespace for each database.
- A `TablespaceHandle` provides access to pg_tablespace. This is implemented but not used.
- An `AttributeHandle` provides access to pg_attribute. pg_attribute is per database. There is an entry (i.e. row) for every column in every table.
- A `TypeHandle` provides access to pg_type. pg_type is per database, and records each available type.  pg_attribute has foreign key references to pg_type.
- An `AttrDefHandle` provides access to pg_attrdef. pg_attrdef is per database and records default values (where defined) for attributes (i.e. columns).
- A `ClassHandle` provides access to pg_class. pg_class is per database, and has a row for anything that is table like, e.g. tables, indexes, etc.

- A `TableHandle` provides a view equivalent to pg_tables, a simpler view of tables. The API here is different since there are multiple underlying tables. 

***

Overview of which catalogs are implemented

[Catalogs](https://docs.google.com/spreadsheets/d/1UBg6RIPQ90fi-fveB9I7Bd3WTokPFz1h_-vfqRCI77s/edit?usp=sharing)

Overview of which views are implemented

[Views](https://docs.google.com/spreadsheets/d/1NIaE0s7mWF48CVHX7tRZBgWCxV3m2VcONFmn1vFgZGI/edit?usp=sharing)

***

Google docs spreadsheets, defining the mapping from Postgres to Terrier. Should be able to add comments (but not edit directly). 

[pg_class](https://docs.google.com/spreadsheets/d/1HfBNyG1prdhDNMy9c6xvXa-wlkmnKSuPKjCommJtX5Q/edit?usp=sharing)

[pg_database](https://docs.google.com/spreadsheets/d/1X8dCqNZ9W5kCJnk_QZu7s3dNXQROvnNIBatY6SsChNs/edit?usp=sharing)

[pg_namespace](https://docs.google.com/spreadsheets/d/1FDyPtyvHM96-ocqrUhW10t1gP6866Wv4-1Y5MQ1RlyA/edit?usp=sharing)

[pg_tablespace](https://docs.google.com/spreadsheets/d/1I8MmWI-8qbGAeyeI47aYktIuynpvfjGILLYLMKhevpw/edit?usp=sharing)