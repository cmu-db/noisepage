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

namespace tpl::sql {
using namespace terrier;

/**
 * This will hold all the pieces needed by the execution engine.
 * i.e Object pools, Txn Manager, Log Manager, Catalog, ...
 */
class ExecutionStructures {
 public:
  static ExecutionStructures *Instance();

  storage::BlockStore *GetBlockStore() { return block_store_.get(); }

  storage::RecordBufferSegmentPool *GetBufferPool() {
    return buffer_pool_.get();
  }

  storage::LogManager *GetLogManager() { return log_manager_.get(); }

  transaction::TransactionManager *GetTxnManager() {
    return txn_manager_.get();
  }

  catalog::Catalog *GetCatalog() { return catalog_.get(); }

  std::shared_ptr<exec::FinalSchema> GetFinalSchema(
      const std::string &name) {
    return test_plan_nodes_.at(name);
  }

 private:
  explicit ExecutionStructures();
  void InitTestTables();
  void InitTestSchemas();
  void InitTestIndexes();
  std::unique_ptr<storage::BlockStore> block_store_;
  std::unique_ptr<storage::RecordBufferSegmentPool> buffer_pool_;
  std::unique_ptr<storage::LogManager> log_manager_;
  std::unique_ptr<transaction::TransactionManager> txn_manager_;
  std::unique_ptr<catalog::Catalog> catalog_;
  std::unordered_map<std::string, std::shared_ptr<exec::FinalSchema>>
      test_plan_nodes_;
};

// Keep small so that nested loop join won't run out of memory.
constexpr u32 test1_size = 10000;
constexpr u32 test2_size = 100;
}  // namespace tpl::sql
