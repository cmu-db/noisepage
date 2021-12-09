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

namespace noisepage::procbench {

/**
 * Class that can load the ProcBench tables, compile the
 * ProcBench queries, and execute the ProcBench workload.
 */
class Workload {
 public:
  /**
   * Construct a new Workload instance.
   * @param db_main The database instance
   * @param db_name The name of the database
   * @param table_root The root of the table data directory
   */
  Workload(common::ManagedPointer<DBMain> db_main, const std::string &db_name, const std::string &table_root);

  /**
   * Function to invoke for a single worker thread to invoke the ProcBench queries.
   * @param exec_mode The execution mode
   */
  void Execute(std::size_t query_number, execution::vm::ExecutionMode exec_mode);

  /** @return The number of queries in the workload. */
  uint32_t GetQueryCount() { return query_and_plan_.size(); }

 private:
  /**
   * Load the tables for the ProcBench benchmark.
   * @param exec_ctx The execution context
   * @param directory The name of the directory from which tables are loaded
   */
  void LoadTables(execution::exec::ExecutionContext *exec_ctx, const std::string &directory);

  /**
   * Load the queries for the ProcBench benchmark.
   * @param accessor The catalog accessor instance
   */
  void LoadQueries(const std::unique_ptr<catalog::CatalogAccessor> &accessor);

  /**
   * Get the index for the specified query number
   * @param query_number The query number
   * @return The index
   */
  std::size_t QueryNumberToIndex(std::size_t query_number) const;

 private:
  /** The database server instance */
  common::ManagedPointer<DBMain> db_main_;
  /** The block store */
  common::ManagedPointer<storage::BlockStore> block_store_;
  /** The catalog instance */
  common::ManagedPointer<catalog::Catalog> catalog_;
  /** The transaction manager */
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  /** The database OID */
  catalog::db_oid_t db_oid_;
  /** The namespace OID */
  catalog::namespace_oid_t ns_oid_;
  /** Execution settings for all executed queries */
  execution::exec::ExecutionSettings exec_settings_{};
  /** The catalog accessor */
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
  /** The collection of executable queries and associated plans */
  std::vector<
      std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>>
      query_and_plan_;
  /** Translate a query number of corresponding index */
  std::unordered_map<std::size_t, std::size_t> query_number_to_index_;
};

}  // namespace noisepage::procbench
