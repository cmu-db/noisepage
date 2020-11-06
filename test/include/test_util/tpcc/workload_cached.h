#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/exec/execution_settings.h"
#include "execution/vm/module.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "test_util/tpcc/database.h"

namespace noisepage::execution::exec {
class ExecutionContext;
}

namespace noisepage::catalog {
class Catalog;
}

namespace noisepage::transaction {
class TransactionManager;
}

namespace noisepage::execution::compiler {
class ExecutableQuery;
}

namespace noisepage {
class DBMain;
}

namespace noisepage::common {
class WorkerPool;
}

namespace noisepage::tpcc {

/**
 * Class that can load the TPCC tables, compile the TPCC queries, and execute the TPCC workload
 */
class WorkloadCached {
 public:
  WorkloadCached(common::ManagedPointer<DBMain> db_main, const std::vector<std::string> &txn_names, int8_t num_threads);

  ~WorkloadCached() { delete tpcc_db_; }

  /**
   * Function to invoke for a single worker thread to invoke the TPCH queries
   * @param worker_id 1-indexed thread id
   */
  void Execute(int8_t worker_id, uint32_t num_precomputed_txns_per_worker, execution::vm::ExecutionMode mode);

 private:
  void GenerateTPCCTables(int8_t num_threads);
  void InitDelivery();
  void InitIndexScan();
  void InitNewOrder();
  void InitOrderStatus();
  void InitPayment();
  void InitSeqScan();
  void InitStockLevel();
  void InitializeSQLs();
  void LoadTPCCQueries(const std::vector<std::string> &txn_names);

  common::ManagedPointer<DBMain> db_main_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  catalog::db_oid_t db_oid_;
  Database *tpcc_db_;

  std::map<std::string, std::vector<std::unique_ptr<execution::compiler::ExecutableQuery>>> queries_;
  std::map<std::string, std::vector<std::string>> sqls_;
  std::vector<std::string> txn_names_;
  execution::exec::ExecutionSettings exec_settings_{};
};

}  // namespace noisepage::tpcc
