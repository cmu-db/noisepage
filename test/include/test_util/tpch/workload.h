#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/compiler/executable_query.h"
#include "common/managed_pointer.h"
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
           transaction::TransactionContext *txn, execution::exec::ExecutionContext *exec_ctx);

 private:
  void GenerateTPCHTables(execution::exec::ExecutionContext *exec_ctx, const std::string &dir_name);

  void LoadTPCHQueries(execution::exec::ExecutionContext *exec_ctx);

  std::vector<parser::ConstantValueExpression> GetQueryParams(const std::string &query_name);

  common::ManagedPointer<DBMain> db_main_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  // catalog::db_oid_t db_oid_;
  catalog::namespace_oid_t ns_oid_;
  std::vector<execution::compiler::ExecutableQuery> queries_;
  execution::exec::SampleOutput sample_output_;
};

}  // namespace terrier::tpch
