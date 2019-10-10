#pragma once

#include <utility>

#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {
/**
 * Helper class for building tables and indexes for postgres catalog.
 */
class Builder {
 public:
  Builder() = delete;

  /**
   * @return schema object for pg_database
   */
  static Schema GetDatabaseTableSchema();

  /**
   * @return schema object for the oid index on pg_database
   */
  static IndexSchema GetDatabaseOidIndexSchema();

  /**
   * @return schema object for the name index on pg_database
   */
  static IndexSchema GetDatabaseNameIndexSchema();

  /**
   * Allocates a new database catalog that roughly conforms to PostgreSQL's catalog layout
   * @param block_store for backing the new catalog tables
   * @param oid of the database which is used for populating the field in redo records
   * @return an initialized DatabaseCatalog
   */
  static DatabaseCatalog *CreateDatabaseCatalog(storage::BlockStore *block_store, db_oid_t oid);

  /**
   * @return schema object for pg_attribute table
   */
  static Schema GetColumnTableSchema();

  /**
   * @return schema object for pg_class table
   */
  static Schema GetClassTableSchema();

  /**
   * @return schema object for pg_constraints table
   */
  static Schema GetConstraintTableSchema();

  /**
   * @return schema object for index table
   */
  static Schema GetIndexTableSchema();

  /**
   * @return schema object for pg_namespace table
   */
  static Schema GetNamespaceTableSchema();

  /**
   * @return schema object for pg_type table
   */
  static Schema GetTypeTableSchema();

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the oid index on pg_namespace
   */
  static IndexSchema GetNamespaceOidIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the name index on pg_namespace
   */
  static IndexSchema GetNamespaceNameIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the oid index on pg_class
   */
  static IndexSchema GetClassOidIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the namespace/name index on pg_class
   */
  static IndexSchema GetClassNameIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the namespace index on pg_class
   */
  static IndexSchema GetClassNamespaceIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the oid index on pg_index
   */
  static IndexSchema GetIndexOidIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for table index on pg_index
   */
  static IndexSchema GetIndexTableIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the table/oid index on pg_attribute
   */
  static IndexSchema GetColumnOidIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the namespace/name index on pg_attribute
   */
  static IndexSchema GetColumnNameIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the oid index on pg_type
   */
  static IndexSchema GetTypeOidIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the namespace/name index on pg_type
   */
  static IndexSchema GetTypeNameIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the namespace index on pg_type
   */
  static IndexSchema GetTypeNamespaceIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the oid index on pg_constraint
   */
  static IndexSchema GetConstraintOidIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the namespace/name index on pg_constraint
   */
  static IndexSchema GetConstraintNameIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the namespace index on pg_constraint
   */
  static IndexSchema GetConstraintNamespaceIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the table index on pg_constraint
   */
  static IndexSchema GetConstraintTableIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the index index on pg_constraint
   */
  static IndexSchema GetConstraintIndexIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the foreign key index on pg_constraint
   */
  static IndexSchema GetConstraintForeignTableIndexSchema(db_oid_t db);

  /**
   * Instantiate a new unique index with the given schema and oid
   * @param key_schema for the index
   * @param oid for the new index
   * @return pointer to the new index
   */
  static storage::index::Index *BuildUniqueIndex(const IndexSchema &key_schema, index_oid_t oid) {
    TERRIER_ASSERT(key_schema.Unique(), "KeySchema must represent a unique index.");
    storage::index::IndexBuilder index_builder;
    index_builder.SetKeySchema(key_schema);
    return index_builder.Build();
  }

  /**
   * Instantiate a new non-unique index with the given schema and oid
   * @param key_schema for the index
   * @param oid for the new index
   * @return pointer to the new index
   */
  static storage::index::Index *BuildLookupIndex(const IndexSchema &key_schema, index_oid_t oid) {
    TERRIER_ASSERT(!(key_schema.Unique()), "KeySchema must represent a non-unique index.");
    storage::index::IndexBuilder index_builder;
    index_builder.SetKeySchema(key_schema);
    return index_builder.Build();
  }
};
}  // namespace terrier::catalog::postgres
