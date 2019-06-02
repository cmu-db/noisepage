#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "execution/exec/output.h"
#include "execution/util/common.h"
#include "storage/garbage_collector.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_manager.h"

namespace tpl::sql {
using terrier::catalog::Catalog;
using terrier::storage::BlockStore;
using terrier::storage::GarbageCollector;
using terrier::storage::LogManager;
using terrier::storage::RecordBufferSegmentPool;
using terrier::transaction::TransactionManager;

using terrier::type::TypeId;
/**
 * This will hold all the pieces needed by the execution engine.
 * i.e Object pools, Txn Manager, Log Manager, Catalog, ...
 * TODO(Amadou): This class was just a convenience singleton class. It should be removed and replace with DBMain.
 */
class ExecutionStructures {
 public:
  /**
   * Static instantiation
   * @return an instance of ExecutionStructures
   */
  static ExecutionStructures *Instance();

  /**
   * @return the block store
   */
  BlockStore *GetBlockStore() { return block_store_.get(); }

  /**
   * @return the record buffer segment pool
   */
  RecordBufferSegmentPool *GetBufferPool() { return buffer_pool_.get(); }

  /**
   * @return the log manager
   */
  LogManager *GetLogManager() { return log_manager_.get(); }

  /**
   * @return the garbage collector
   */
  GarbageCollector *GetGC() { return gc_.get(); }

  /**
   * @return the transaction manager
   */
  TransactionManager *GetTxnManager() { return txn_manager_.get(); }

  /**
   * @return the catalog
   */
  Catalog *GetCatalog() { return catalog_.get(); }

  /**
   * Used for testing only
   * @param name name of test the final schema
   * @return associated final schema
   */
  std::shared_ptr<exec::FinalSchema> GetFinalSchema(const std::string &name) { return test_plan_nodes_.at(name); }

  /**
   * Return the test db and namespace oid.
   */
  std::pair<terrier::catalog::db_oid_t, terrier::catalog::namespace_oid_t> GetTestDBAndNS() {
    return {test_db_oid_, test_ns_oid_};
  }

 private:
  ExecutionStructures();
  void InitTestTables(terrier::transaction::TransactionContext *txn);
  void InitTPCHOutputSchemas(terrier::transaction::TransactionContext * txn);
  void InitTestSchemas(terrier::transaction::TransactionContext *txn);
  void InitTestIndexes(terrier::transaction::TransactionContext *txn);
  std::unique_ptr<BlockStore> block_store_;
  std::unique_ptr<RecordBufferSegmentPool> buffer_pool_;
  std::unique_ptr<LogManager> log_manager_;
  std::unique_ptr<TransactionManager> txn_manager_;
  std::unique_ptr<Catalog> catalog_;
  std::unordered_map<std::string, std::shared_ptr<exec::FinalSchema>> test_plan_nodes_;
  std::unique_ptr<GarbageCollector> gc_;
  terrier::catalog::db_oid_t test_db_oid_ = terrier::catalog::DEFAULT_DATABASE_OID;
  terrier::catalog::namespace_oid_t test_ns_oid_;
};

// Keep small so that nested loop join won't run out of memory.
/**
 * Size of the first table
 */
constexpr u32 test1_size = 10000;
/**
 * Size of the second table
 */
constexpr u32 test2_size = 100;
}  // namespace tpl::sql
