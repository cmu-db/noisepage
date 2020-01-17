#pragma once

#include <utility>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "storage/storage_defs.h"
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

namespace terrier::execution {
class ExecutableQuery;
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
  void Execute(int8_t worker_id, uint32_t num_precomputed_txns_per_worker, execution::vm::ExecutionMode mode);

 private:
  void GenerateTPCHTables(execution::exec::ExecutionContext *exec_ctx, const std::string &table_root);

  void LoadTPCHQueries(execution::exec::ExecutionContext *exec_ctx, const std::vector<std::string> &queries);

  common::ManagedPointer<DBMain> db_main_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  catalog::db_oid_t db_oid_;
  catalog::namespace_oid_t ns_oid_;

  std::vector<execution::ExecutableQuery> queries_;
};

}  // namespace terrier::tpch
