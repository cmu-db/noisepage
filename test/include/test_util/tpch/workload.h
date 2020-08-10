#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/executable_query.h"
#include "execution/table_generator/sample_output.h"
#include "execution/vm/module.h"

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

  std::vector<parser::ConstantValueExpression> GetQueryParams(const std::string &query_name);

  common::ManagedPointer<DBMain> db_main_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  catalog::db_oid_t db_oid_;
  catalog::namespace_oid_t ns_oid_;

  std::vector<execution::ExecutableQuery> queries_;
  execution::exec::SampleOutput sample_output_;
};

}  // namespace terrier::tpch
