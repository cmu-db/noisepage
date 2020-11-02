#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_settings.h"
#include "execution/vm/module.h"

namespace noisepage::execution::exec {
class ExecutionContext;
}

namespace noisepage::catalog {
class Catalog;
}

namespace noisepage::transaction {
class TransactionManager;
}

namespace noisepage {
class DBMain;
}

namespace noisepage::tpch {

/**
 * Class that can load the TPCH tables, compile the TPCH queries, and execute the TPCH workload
 */
class Workload {
 public:
  enum class BenchmarkType : uint32_t { TPCH, SSB };

  Workload(common::ManagedPointer<DBMain> db_main, const std::string &db_name, const std::string &table_root,
           enum BenchmarkType type);

  /**
   * Function to invoke for a single worker thread to invoke the TPCH queries
   * @param worker_id 1-indexed thread id
   */
  void Execute(int8_t worker_id, uint64_t execution_us_per_worker, uint64_t avg_interval_us, uint32_t query_num,
               execution::vm::ExecutionMode mode);
  uint32_t GetQueryNum() { return query_and_plan_.size(); }

 private:
  void GenerateTables(execution::exec::ExecutionContext *exec_ctx, const std::string &dir_name,
                      enum BenchmarkType type);

  void LoadQueries(const std::unique_ptr<catalog::CatalogAccessor> &accessor, enum BenchmarkType type);

  common::ManagedPointer<DBMain> db_main_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  catalog::db_oid_t db_oid_;
  catalog::namespace_oid_t ns_oid_;
  execution::exec::ExecutionSettings exec_settings_{};
  std::unique_ptr<catalog::CatalogAccessor> accessor_;

  std::vector<
      std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>>
      query_and_plan_;
};

}  // namespace noisepage::tpch
