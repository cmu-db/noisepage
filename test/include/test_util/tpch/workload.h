#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_settings.h"
#include "execution/table_generator/sample_output.h"
#include "execution/vm/module.h"
#include "catalog/catalog_accessor.h"
namespace terrier::execution::exec {
class ExecutionContext;
}

namespace terrier::catalog {
class Catalog;
}

namespace terrier::transaction {
class TransactionManager;
}

namespace terrier {
class DBMain;
}

namespace terrier::tpch {

/**
 * Class that can load the TPCH tables, compile the TPCH queries, and execute the TPCH workload
 */
class Workload {
 public:
  Workload(common::ManagedPointer<DBMain> db_main, const std::string &db_name, const std::string &table_root,
           const std::vector<std::string> &queries);

  /**
   * Function to invoke for a single worker thread to invoke the TPCH queries
   * @param worker_id 1-indexed thread id
   */
  void Execute(int8_t worker_id, uint64_t execution_us_per_worker, uint64_t avg_interval_us, uint32_t query_num,
               execution::vm::ExecutionMode mode);

 private:
  void GenerateTPCHTables(execution::exec::ExecutionContext *exec_ctx, const std::string &dir_name);

  void LoadTPCHQueries(execution::exec::ExecutionContext *exec_ctx, const std::vector<std::string> &queries);

  void LoadTPCHQueries();
  void MakeExecutableQ1();
  void MakeExecutableQ4();
  void MakeExecutableQ5();
  void MakeExecutableQ6();
  void MakeExecutableQ7();
  void MakeExecutableQ11();
  void MakeExecutableQ18();
  std::vector<parser::ConstantValueExpression> GetQueryParams(const std::string &query_name);

  common::ManagedPointer<DBMain> db_main_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  catalog::db_oid_t db_oid_;
  catalog::namespace_oid_t ns_oid_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;

  std::vector<execution::compiler::ExecutableQuery> queries_;
  std::vector<std::string> query_names_;
  execution::exec::SampleOutput sample_output_;
  execution::exec::ExecutionSettings exec_settings_{};
  std::vector<std::unique_ptr<execution::compiler::ExecutableQuery>> queries_ptr_;
  std::unique_ptr<execution::compiler::ExecutableQuery> q1_ = nullptr;
  std::unique_ptr<planner::AbstractPlanNode> q1_last_op_ = nullptr;

  std::unique_ptr<execution::compiler::ExecutableQuery> q4_ = nullptr;
  std::unique_ptr<planner::AbstractPlanNode> q4_last_op_ = nullptr;
  std::unique_ptr<execution::compiler::ExecutableQuery> q5_ = nullptr;
  std::unique_ptr<planner::AbstractPlanNode> q5_last_op_ = nullptr;
  std::unique_ptr<execution::compiler::ExecutableQuery> q6_ = nullptr;
  std::unique_ptr<planner::AbstractPlanNode> q6_last_op_ = nullptr;
  std::unique_ptr<execution::compiler::ExecutableQuery> q7_ = nullptr;
  std::unique_ptr<planner::AbstractPlanNode> q7_last_op_ = nullptr;
  std::unique_ptr<execution::compiler::ExecutableQuery> q11_ = nullptr;
  std::unique_ptr<planner::AbstractPlanNode> q11_last_op_ = nullptr;

  std::unique_ptr<execution::compiler::ExecutableQuery> q18_ = nullptr;
  std::unique_ptr<planner::AbstractPlanNode> q18_last_op_ = nullptr;
};

}  // namespace terrier::tpch
