#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include "catalog/catalog_defs.h"
#include "catalog/catalog_index.h"
#include "catalog/catalog_sql_table.h"
#include "storage/index/index.h"
#include "storage/record_buffer.h"

namespace terrier::catalog {
/**
 * Catalog.
 */
class Catalog {
 public:
  /**
   * Constructor
   * @param txn_manager the transaction manager
   */
  explicit Catalog(transaction::TransactionManager *txn_manager) : oid_{0} {}

  /**
   * Constructor
   * @param txn_manager the transaction manager
   * @param block_store block store to use
   */
  explicit Catalog(transaction::TransactionManager *txn_manager, storage::BlockStore *block_store)
      : oid_{0}, block_store_(block_store) {}

  /**
   * Create a table with schema
   * @param txn transaction to use
   * @param db_oid oid of the database
   * @param table_name table name
   * @param schema schema to use
   */
  table_oid_t CreateTable(transaction::TransactionContext *txn, db_oid_t db_oid, const std::string &table_name,
                          const Schema &schema);

  /**
   * Creates an index
   * @param txn transaction to use
   * @param constraint_type type of the constraint
   * @param schema schema for this index's key
   * @param name of the index
   * @return oid of the created index.
   */
  index_oid_t CreateIndex(transaction::TransactionContext *txn, storage::index::ConstraintType constraint_type,
                          const storage::index::IndexKeySchema &schema, const std::string &name = "index");

  /**
   * Delete a table
   * @param txn transaction to use
   * @param db_oid oid of the database
   * @param table_oid table to delete
   */

  void DeleteTable(transaction::TransactionContext *txn, db_oid_t db_oid, table_oid_t table_oid);

  /**
   * Get a pointer to the storage table.
   * Supports both catalog tables, and user created tables.
   *
   * @param db_oid database that owns the table
   * @param table_oid returns the storage table pointer for this table_oid
   * @return a pointer to the catalog
   * @throw out_of_range exception if either oid doesn't exist or the catalog
   * doesn't exist.
   */
  std::shared_ptr<catalog::SqlTableRW> GetCatalogTable(db_oid_t db_oid, table_oid_t table_oid);

  /**
   * Get a pointer to an index
   * @param index_oid oid of the index
   * @return pointer to the index
   */
  std::shared_ptr<CatalogIndex> GetCatalogIndex(index_oid_t index_oid);

  /**
   * Get an index oid
   * @param name name of the index
   * @return oid of the index
   */
  index_oid_t GetCatalogIndexOid(const std::string &name);

  /**
   * Get a pointer to the storage table, by table_name.
   * Supports both catalog tables, and user created tables.
   *
   * @param db_oid database that owns the table
   * @param table_name returns the storage table point for this table
   * @return a pointer to the catalog
   * @throw out_of_range exception if either oid doesn't exist or the catalog
   * doesn't exist.
   */
  std::shared_ptr<catalog::SqlTableRW> GetCatalogTable(db_oid_t db_oid, const std::string &table_name);

  /**
   * The global counter for getting next oid. The return result should be
   * converted into corresponding oid type
   *
   * This function is atomic.
   *
   * @return uint32_t the next oid available
   */
  uint32_t GetNextOid() { return ++oid_; }

  /**
   * Add a catalog to the catalog mapping
   * @param db_oid database oid
   * @param table_oid table oid
   * @param name of the catalog
   * @param table_rw_p catalog storage table
   */
  void AddToMaps(db_oid_t db_oid, table_oid_t table_oid, const std::string &name,
                 std::shared_ptr<SqlTableRW> table_rw_p) {
    map_[db_oid][table_oid] = std::move(table_rw_p);
    name_map_[db_oid][name] = table_oid;
  }

 private:
  // map from (db_oid, catalog table_oid_t) to sql table rw wrapper
  std::unordered_map<db_oid_t, std::unordered_map<table_oid_t, std::shared_ptr<catalog::SqlTableRW>>> map_;
  // map from (db_oid, catalog name) to table_oid
  std::unordered_map<db_oid_t, std::unordered_map<std::string, table_oid_t>> name_map_;

  // map from catalog index_oid_t to index
  std::unordered_map<index_oid_t, std::shared_ptr<catalog::CatalogIndex>> index_map_;
  std::unordered_map<std::string, index_oid_t> index_names_;

  // this oid serves as a global counter for different strong types of oid
  std::atomic<uint32_t> oid_;
  storage::BlockStore *block_store_;
};
}  // namespace terrier::catalog
