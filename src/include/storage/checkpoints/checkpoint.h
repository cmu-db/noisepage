#pragma once

#include <catalog/catalog.h>
#include <common/managed_pointer.h>
#include <stdio.h>
#include <mutex>
#include <string>

#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_database.h"
#include "catalog/postgres/pg_index.h"
#include "common/worker_pool.h"
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage {
/**
 * The Checkpoint class is responsible for taking checkpoints for a single database. The flow of checkpoint taking is
 * as follows:
 *      1. checkpoint class being constructed and called by a background thread
 *      2. filter the log file to separate logs related to catalog tables
 *      3. write data of user tables to disk using ArrowSerializer
 */
class Checkpoint {
 public:
  Checkpoint(const common::ManagedPointer<catalog::Catalog> catalog,
             common::ManagedPointer<transaction::TransactionManager> txn_manager,
             common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager,
             common::ManagedPointer<storage::GarbageCollector> gc,
             common::ManagedPointer<storage::LogManager> log_manager)
      : catalog_(catalog),
        txn_manager_(txn_manager),
        deferred_action_manager_(deferred_action_manager),
        gc_(gc),
        log_manager_(log_manager) {
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
   * @param path the path on disk to save the checkpoint
   * @param db the database to take the checkpoint of
   * @param num_threads the number of threads used for thread_pool
   * @param thread_pool_ the thread pool used for checkpoint taking
   * @return True if succuessully take the checkpoint, False otherwise
   */
  bool TakeCheckpoint(const std::string &path, catalog::db_oid_t db, const char *cur_log_file, uint32_t num_threads,
                      common::WorkerPool *thread_pool_);

  /**
   * Generate a file name for a table
   * @tparam db_oid the oid of the database the table belongs to
   * @param tb_oid the oid of the table
   * @return a file name in the format db_oid-tb_oid.txt
   */
  static std::string GenFileName(catalog::db_oid_t db_oid, catalog::table_oid_t tb_oid) {
    return std::to_string((uint32_t)db_oid) + "-" + std::to_string((uint32_t)tb_oid);
  }

  /**
   * Get the db_oid and tb_oit from the file name
   * @param file_name the name of the checkpoint file
   * @param db_oid the oid of the database the table belongs to
   * @param tb_oid the oid of the table
   * @return a file name in the format db_oid-tb_oid.txt
   */
  static void GenOidFromFileName(std::string file_name, catalog::db_oid_t &db_oid, catalog::table_oid_t &tb_oid) {
    auto sep_ind = file_name.find("-");
    db_oid = (catalog::db_oid_t)std::stoi(file_name.substr(0, sep_ind));
    tb_oid = (catalog::table_oid_t)std::stoi(file_name.substr(sep_ind + 1, file_name.length()));
  }

  /**
   * Splits a string into a vector
   * @param s string to split
   * @param delimiter that separates the string
   * @return vector of strings
   */
  static const std::vector<std::string> StringSplit(const std::string &s, const char &delimiter) {
    std::string buff{""};
    std::vector<std::string> str_vec;

    for (auto c : s) {
      if (c != delimiter)
        buff += c;
      else if (c == delimiter && buff != "") {
        str_vec.push_back(buff);
        buff = "";
      }
    }
    if (buff != "") str_vec.push_back(buff);

    return str_vec;
  }

 private:
  // Catalog to fetch table pointers
  friend class CheckpointBackgroundLoop;
  const common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager_;
  common::ManagedPointer<storage::GarbageCollector> gc_;
  common::ManagedPointer<storage::LogManager> log_manager_;
  std::unordered_map<catalog::table_oid_t, catalog::Schema> catalog_table_schemas_;
  std::vector<std::pair<catalog::table_oid_t, storage::DataTable *>> queue;  // for multithreading
  std::mutex queue_latch;

  /**
   * Write the data of a database to disk in parallel, called by TakeCheckpoint()
   * @param path the path on disk to save the checkpoint
   * @param accessor catalog accessor of the given database
   * @param db_oid the databse to be checkpointed
   * @return None
   */
  void WriteToDisk(const std::string &path, const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                   catalog::db_oid_t db_oid);

  /**
 * Filter the logs in original log file to separate logs related to catalog tables to another file
 * @param old_log_path the path of the original log file
 * @param new_log_path the path of the new log file to save the logs related to catalog tables
 * @return None
 */
  void FilterCatalogLogs(const std::string &old_log_path, const std::string &new_log_path);


  };  // class checkpoint

}  // namespace terrier::storage