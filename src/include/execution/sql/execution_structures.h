#pragma once

#include <memory>
#include <unordered_map>
#include "catalog/catalog.h"
#include "execution/exec/output.h"
#include "storage/garbage_collector.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_manager.h"
#include "execution/util/common.h"
#include "storage/garbage_collector.h"

namespace tpl::sql {
using terrier::storage::BlockStore;
using terrier::storage::RecordBufferSegmentPool ;
using terrier::storage::LogManager;
using terrier::storage::GarbageCollector;
using terrier::transaction::TransactionManager;
using terrier::catalog::Catalog;

using terrier::type::TypeId;
/**
 * This will hold all the pieces needed by the execution engine.
 * i.e Object pools, Txn Manager, Log Manager, Catalog, ...
 */
class ExecutionStructures {
 public:
  static ExecutionStructures *Instance();

  BlockStore *GetBlockStore() { return block_store_.get(); }

  RecordBufferSegmentPool *GetBufferPool() {
    return buffer_pool_.get();
  }

  LogManager *GetLogManager() { return log_manager_.get(); }

  GarbageCollector *GetGC() { return gc_.get(); }

  TransactionManager *GetTxnManager() {
    return txn_manager_.get();
  }

  Catalog *GetCatalog() { return catalog_.get(); }

  std::shared_ptr<exec::FinalSchema> GetFinalSchema(
      const std::string &name) {
    return test_plan_nodes_.at(name);
  }

 private:
  explicit ExecutionStructures();
  void InitTestTables();
  void InitTestSchemas();
  void InitTestIndexes();
  std::unique_ptr<BlockStore> block_store_;
  std::unique_ptr<RecordBufferSegmentPool> buffer_pool_;
  std::unique_ptr<LogManager> log_manager_;
  std::unique_ptr<TransactionManager> txn_manager_;
  std::unique_ptr<Catalog> catalog_;
  std::unordered_map<std::string, std::shared_ptr<exec::FinalSchema>>
      test_plan_nodes_;
  std::unique_ptr<GarbageCollector> gc_;
};

// Keep small so that nested loop join won't run out of memory.
constexpr u32 test1_size = 10000;
constexpr u32 test2_size = 100;
}  // namespace tpl::sql
