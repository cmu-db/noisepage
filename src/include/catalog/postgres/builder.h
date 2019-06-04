#pragma once

#include <utility>

#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "storage/storage_defs.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {
class Builder {
 public:
  Builder() = delete;

  /**
   * Allocates a new database catalog that roughly conforms to PostgreSQL's catalog layout
   * @param block_store for backing the new catalog tables
   * @return an initialized DatabaseCatalog
   */
  static DatabaseCatalog *AllocateCatalog(storage::BlockStore block_store);

  /**
   * Bootstraps the catalog's own metadata into itself
   * @param txn for the operations
   * @param catalog to bootstrap
   * @return an initialized DatabaseCatalog
   */
  static void BootstrapCatalog(transaction::TransactionContext *txn, DatabaseCatalog catalog);

  /**
   * Get the schema for pg_attribute
   * @return schema object for pg_attribute table
   */
  static Schema GetAttributeTableSchema();

  /**
   * Get the schema for pg_class
   * @return schema object for pg_class table
   */
  static Schema GetClassTableSchema();

  /**
   * Get the schema for pg_constraint
   * @return schema object for pg_constraints table
   */
  static Schema GetConstraintTableSchema();

  /**
   * Get the schema for pg_index
   * @return schema object for index table
   */
  static Schema GetIndexTableSchema();

  /**
   * Get the schema for pg_namespace
   * @return schema object for pg_namespace table
   */
  static Schema GetNamespaceTableSchema();

  /**
   * Get the schema for pg_type
   * @return schema object for pg_type table
   */
  static Schema GetTypeTableSchema();

  static IndexSchema GetNamepaceOidIndexSchema();
  static IndexSchema GetNamespaceNameIndexSchema();
  static IndexSchema GetClassOidIndexSchema();
  static IndexSchema GetClassNameIndexSchema();
  static IndexSchema GetIndexOidIndexSchema();
  static IndexSchema GetIndexTableIndexSchema();
  static IndexSchema GetColumnOidIndexSchema();
  static IndexSchema GetColumnNameIndexSchema();
  static IndexSchema GetTypeOidIndexSchema();
  static IndexSchema GetTypeNameIndexSchema();
  static IndexSchema GetConstaintOidIndexSchema();
  static IndexSchema GetConstraintNameIndexSchema();
  static IndexSchema GetConstraintTableIndexSchema();
  static IndexSchema GetConstraintIndexIndexSchema();
  static IndexSchema GetConstraintForeignKeyIndexSchema();

};
}
