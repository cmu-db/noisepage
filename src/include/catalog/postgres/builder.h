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
   * @return an initialized DatabaseCatalog
   */
  static DatabaseCatalog *CreateDatabaseCatalog(storage::BlockStore *block_store);

  /**
   * Bootstraps the catalog's own metadata into itself
   * @param txn for the operations
   * @param catalog to bootstrap
   * @return an initialized DatabaseCatalog
   */
  static void BootstrapDatabaseCatalog(transaction::TransactionContext *txn, DatabaseCatalog *catalog);

  /**
   * @return schema object for pg_attribute table
   */
  static Schema GetAttributeTableSchema();

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
   * @return schema object for the oid index on pg_namespace
   */
  static IndexSchema GetNamepaceOidIndexSchema();

  /**
   * @return schema object for the name index on pg_namespace
   */
  static IndexSchema GetNamespaceNameIndexSchema();

  /**
   * @return schema object for the oid index on pg_class
   */
  static IndexSchema GetClassOidIndexSchema();

  /**
   * @return schema object for the namespace/name index on pg_class
   */
  static IndexSchema GetClassNameIndexSchema();

  /**
   * @return schema object for the namespace index on pg_class
   */
  static IndexSchema GetClassNamespaceIndexSchema();

  /**
   * @return schema object for the oid index on pg_index
   */
  static IndexSchema GetIndexOidIndexSchema();

  /**
   * @return schema object for table index on pg_index
   */
  static IndexSchema GetIndexTableIndexSchema();

  /**
   * @return schema object for the table/oid index on pg_attribute
   */
  static IndexSchema GetColumnOidIndexSchema();

  /**
   * @return schema object for the namespace/name index on pg_attribute
   */
  static IndexSchema GetColumnNameIndexSchema();

  /**
   * @return schema object for the class (index and table) index on pg_attribute
   */
  static IndexSchema GetColumnClassIndexSchema();

  /**
   * @return schema object for the oid index on pg_type
   */
  static IndexSchema GetTypeOidIndexSchema();

  /**
   * @return schema object for the namespace/name index on pg_type
   */
  static IndexSchema GetTypeNameIndexSchema();

  /**
   * @return schema object for the namespace index on pg_type
   */
  static IndexSchema GetTypeNamespaceIndexSchema();

  /**
   * @return schema object for the oid index on pg_constraint
   */
  static IndexSchema GetConstraintOidIndexSchema();

  /**
   * @return schema object for the namespace/name index on pg_constraint
   */
  static IndexSchema GetConstraintNameIndexSchema();

  /**
   * @return schema object for the namespace index on pg_constraint
   */
  static IndexSchema GetConstraintNamespaceIndexSchema();

  /**
   * @return schema object for the table index on pg_constraint
   */
  static IndexSchema GetConstraintTableIndexSchema();

  /**
   * @return schema object for the index index on pg_constraint
   */
  static IndexSchema GetConstraintIndexIndexSchema();

  /**
   * @return schema object for the foreign key index on pg_constraint
   */
  static IndexSchema GetConstraintForeignKeyIndexSchema();

  /**
   * Instantiate a new unique index with the given schema and oid
   * @param key_schema for the index
   * @param oid for the new index
   * @return pointer to the new index
   */
  static storage::index::Index *BuildUniqueIndex(const IndexSchema &key_schema, index_oid_t oid) {
    storage::index::IndexBuilder index_builder;
    index_builder.SetOid(oid).SetKeySchema(key_schema).SetConstraintType(storage::index::ConstraintType::UNIQUE);
    return index_builder.Build();
  }

  /**
   * Instantiate a new non-unique index with the given schema and oid
   * @param key_schema for the index
   * @param oid for the new index
   * @return pointer to the new index
   */
  storage::index::Index *BuildLookupIndex(const IndexSchema &key_schema, index_oid_t oid) {
    storage::index::IndexBuilder index_builder;
    index_builder.SetOid(oid).SetKeySchema(key_schema).SetConstraintType(storage::index::ConstraintType::DEFAULT);
    return index_builder.Build();
  }
};
}  // namespace terrier::catalog::postgres
