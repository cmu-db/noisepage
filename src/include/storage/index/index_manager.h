#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "loggers/main_logger.h"
#include "parser/parser_defs.h"
#include "storage/index/index.h"
#include "storage/index/index_factory.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage::index {

/**
 * An index manager is a class that creates an index on key attributes specified by users. It can create
 * the index both in the non-blocking manner and the blocking manner, which is also determined by users.
 *
 * This class can only be used when creating an index.
 */
class IndexManager {
 private:
  /**
   * The method sets the type of constraints and the key schema for the index. It finally returns an empty
   * index for further insertion.
   *
   * @param index_oid the oid of the index given by catalog
   * @param sql_table the target sql table
   * @param unique_index whether the index has unique constraint or not
   * @param key_attrs all attributes in the key
   * @return an empty index with metadata set
   */
  Index *GetEmptyIndex(catalog::index_oid_t index_oid, SqlTable *sql_table, bool unique_index,
                       const std::vector<std::string> &key_attrs);

  /**
   * Record the oid of db, the oid of namespace and the oid of index.
   */
  // FIXME(jiaqizuo): The data struture is not good and should use more clear data structure later
  using index_id_t = std::pair<std::pair<catalog::db_oid_t, catalog::namespace_oid_t>, catalog::index_oid_t>;

  /**
   * A map from index_id to the status of the index (in the process of building or not)
   */
  std::map<index_id_t, bool> index_building_map_;
  // FIXME(xueyuanz): This latch might not be necessary, the index_builing_map_ is also guarded by the commit_latch.
  common::SpinLatch index_building_map_latch_;

 public:
  /**
   * Create the index_id_t using the oid of db, the oid of namespace and the oid of the index
   * @param db_oid the oid of database
   * @param namespace_oid the oid of namespace
   * @param index_oid the oid of the index
   * @return the result data structure with the type of index_id_t
   */
  index_id_t make_index_id(catalog::db_oid_t db_oid, catalog::namespace_oid_t namespace_oid,
                           catalog::index_oid_t index_oid) {
    return std::make_pair(std::make_pair(db_oid, namespace_oid), index_oid);
  }

  /**
   * Set the status of index as building in the flag
   *
   * @param key the index_id representing the index
   * @param value true if the index is in the process of building otherwise false
   */
  void SetIndexBuildingFlag(const index_id_t &key, bool value) {
    common::SpinLatch::ScopedSpinLatch guard(&index_building_map_latch_);
    index_building_map_[key] = value;
  }

  /**
   * Get the status of index as building in the flag
   *
   * @param key the index_id representing the index
   * @return -1 if the index is not being built, 1 if the flag is set, and 0 if the flag is not set.
   */
  int GetIndexBuildingFlag(const index_id_t &key) {
    common::SpinLatch::ScopedSpinLatch guard(&index_building_map_latch_);
    auto it = index_building_map_.find(key);
    if (it == index_building_map_.end()) return -1;
    return it->second ? 1 : 0;
  }

  /**
   * The method can create the index in a non-blocking manner. It launches two transactions to build the index.
   * The first transaction inserts a new entry on the index into catalog and creates an empty index with all metadata
   * set. The second transaction inserts all keys in its snapshot into the index. Before the start of second
   * transaction, it needs to wait for all other transactions with timestamp smaller than the commit timestamp of the
   * first transaction to complete.
   *
   * @param db_oid the oid of the database
   * @param ns_oid the oid of the namespace
   * @param table_oid the oid of the table
   * @param index_type the type of the index
   * @param unique_index whether the index is unique
   * @param index_name the name of the index
   * @param index_attrs all attributes indexed on
   * @param key_attrs all attributes of the key
   * @param txn_mgr the pointer to the transaction manager
   * @param catalog the pointer to the catalog
   * @return the index oid if create success otherwise 0
   */
  catalog::index_oid_t CreateConcurrently(catalog::db_oid_t db_oid, catalog::namespace_oid_t ns_oid,
                                          catalog::table_oid_t table_oid, parser::IndexType index_type,
                                          bool unique_index, const std::string &index_name,
                                          const std::vector<std::string> &index_attrs,
                                          const std::vector<std::string> &key_attrs,
                                          transaction::TransactionManager *txn_mgr, catalog::Catalog *catalog);

  /**
   * Drop the index. The method first deletes the entry about the index in the catalog in a transaction.
   * Then, it waits for all running transactions still using the index to complete.
   *
   * @param db_oid the oid of the database
   * @param ns_oid the oid of the namespace
   * @param table_oid the oid of the table
   * @param index_oid the oid of the index
   * @param index_name the name of the index
   * @param txn_mgr the pointer to the transaction manager
   * @param catalog the pointer to the catalog
   */
  void Drop(catalog::db_oid_t db_oid, catalog::namespace_oid_t ns_oid, catalog::table_oid_t table_oid,
            catalog::index_oid_t index_oid, const std::string &index_name, transaction::TransactionManager *txn_mgr,
            catalog::Catalog *catalog);

  /**
   * The method populates all tuples in the table visible to current transaction with latest version,
   * then pack target columns into ProjectedRow's and insert into the given index.
   *
   * @param txn current transaction context
   * @param sql_table the target sql table
   * @param index target index inserted into
   * @param unique_index whether the index is unique or not
   * @return true if populating index successes otherwise false
   */
  bool PopulateIndex(transaction::TransactionContext *txn, const SqlTable &sql_table, Index *index, bool unique_index);
};
}  // namespace terrier::storage::index
