#pragma once

#include <utility>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "storage/storage_defs.h"

namespace noisepage::catalog {
class DatabaseCatalog;
class IndexSchema;
class Schema;
}  // namespace noisepage::catalog

namespace noisepage::storage {
class GarbageCollector;
}  // namespace noisepage::storage

namespace noisepage::storage::index {
class Index;
}  // namespace noisepage::storage::index

namespace noisepage::catalog::postgres {
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
   * @param garbage_collector injected GC to register and deregister indexes. Temporary?
   * @return an initialized DatabaseCatalog
   */
  static DatabaseCatalog *CreateDatabaseCatalog(common::ManagedPointer<storage::BlockStore> block_store, db_oid_t oid,
                                                common::ManagedPointer<storage::GarbageCollector> garbage_collector);

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
   * @return schema object for pg_language table
   */
  static Schema GetLanguageTableSchema();

  /**
   * @return schema object for pg_proc table
   */
  static Schema GetProcTableSchema();

  /**
   * @return schema object for pg_statistic table
   */
  static Schema GetStatisticTableSchema();

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
   * @param db oid in which the indexed table exists
   * @return schema object for the oid index on pg_language
   */
  static IndexSchema GetLanguageOidIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the name index on pg_language
   */
  static IndexSchema GetLanguageNameIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the oid index on pg_proc
   */
  static IndexSchema GetProcOidIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the name index on pg_proc
   */
  static IndexSchema GetProcNameIndexSchema(db_oid_t db);

  /**
   * @param db oid in which the indexed table exists
   * @return schema object for the table/oid index on pg_statistic
   */
  static IndexSchema GetStatisticOidIndexSchema(db_oid_t db);

  /**
   * Instantiate a new unique index with the given schema and oid
   * @param key_schema for the index
   * @param oid for the new index
   * @return pointer to the new index
   */
  static storage::index::Index *BuildUniqueIndex(const IndexSchema &key_schema, index_oid_t oid);

  /**
   * Instantiate a new non-unique index with the given schema and oid
   * @param key_schema for the index
   * @param oid for the new index
   * @return pointer to the new index
   */
  static storage::index::Index *BuildLookupIndex(const IndexSchema &key_schema, index_oid_t oid);
};
}  // namespace noisepage::catalog::postgres
