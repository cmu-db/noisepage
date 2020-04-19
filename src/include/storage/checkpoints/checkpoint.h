#pragma once

#include <catalog/catalog.h>
#include <common/managed_pointer.h>
#include <mutex>
#include <string>
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_database.h"
#include "catalog/postgres/pg_index.h"
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage {

class Checkpoint {
 public:
  Checkpoint(const common::ManagedPointer<catalog::Catalog> catalog,
             common::ManagedPointer<transaction::TransactionManager> txn_manager,
             common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager,
             common::ManagedPointer<storage::GarbageCollector> gc)
      : catalog_(catalog), txn_manager_(txn_manager), deferred_action_manager_(deferred_action_manager), gc_(gc) {
    // Initialize catalog_table_schemas_ map
    catalog_table_schemas_[catalog::postgres::CLASS_TABLE_OID] = catalog::postgres::Builder::GetClassTableSchema();
    catalog_table_schemas_[catalog::postgres::NAMESPACE_TABLE_OID] =
        catalog::postgres::Builder::GetNamespaceTableSchema();
    catalog_table_schemas_[catalog::postgres::COLUMN_TABLE_OID] = catalog::postgres::Builder::GetColumnTableSchema();
    catalog_table_schemas_[catalog::postgres::CONSTRAINT_TABLE_OID] =
        catalog::postgres::Builder::GetConstraintTableSchema();
    catalog_table_schemas_[catalog::postgres::INDEX_TABLE_OID] = catalog::postgres::Builder::GetIndexTableSchema();
    catalog_table_schemas_[catalog::postgres::TYPE_TABLE_OID] = catalog::postgres::Builder::GetTypeTableSchema();
  }

  /**
   * Take checkpoint of a database
   * @tparam path the path on disk to save the checkpoint
   * @param db the database to take the checkpoint of
   * @return True if succuessully take the checkpoint, False otherwise
   */
  bool TakeCheckpoint(const std::string &path, catalog::db_oid_t db);

  /**
   * Generate a file name for a table
   * @tparam db_oid the oid of the database the table belongs to
   * @param tb_oid the oid of the table
   * @return a file name in the format db_oid-tb_oid.txt
   */
  static std::string GenFileName(catalog::db_oid_t db_oid, catalog::table_oid_t tb_oid) {
    return std::to_string((uint32_t)db_oid) + "-" + std::to_string((uint32_t)tb_oid);
  }

  static void GenOidFromFileName(std::string file_name, catalog::db_oid_t &db_oid, catalog::table_oid_t &tb_oid) {
    auto sep_ind = file_name.find("-");
    db_oid = (catalog::db_oid_t)std::stoi(file_name.substr(0, sep_ind));
    tb_oid = (catalog::table_oid_t)std::stoi(file_name.substr(sep_ind + 1, file_name.length()));
  }

  static const std::vector<std::string> StringSplit(const std::string &s, const char &c) {
    std::string buff{""};
    std::vector<std::string> v;

    for (auto n : s) {
      if (n != c)
        buff += n;
      else if (n == c && buff != "") {
        v.push_back(buff);
        buff = "";
      }
    }
    if (buff != "") v.push_back(buff);

    return v;
  }

 private:
  // Catalog to fetch table pointers
  const common::ManagedPointer<catalog::Catalog> catalog_;
  //  const common::ManagedPointer<BlockStore> block_store_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager_;
  common::ManagedPointer<storage::GarbageCollector> gc_;
  std::unordered_map<catalog::table_oid_t, catalog::Schema> catalog_table_schemas_;
  std::vector<catalog::table_oid_t> queue;  // for multithreading
  std::mutex queue_latch;

  /**
   * Write the data of a database to disk in parallel, called by TakeCheckpoint()
   * @tparam path the path on disk to save the checkpoint
   * @param accessor catalog accessor of the given database
   * @param db_oid the databse to be checkpointed
   * @return None
   */
  void WriteToDisk(const std::string &path, const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                   catalog::db_oid_t db_oid);

};  // class checkpoint

}  // namespace terrier::storage