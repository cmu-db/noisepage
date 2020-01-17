#include "test_util/tpch/workload.h"

#include <random>

#include "execution/exec/execution_context.h"
#include "execution/execution_util.h"
#include "execution/table_generator/table_generator.h"
#include "main/db_main.h"

namespace terrier::tpch {

Workload::Workload(common::ManagedPointer<DBMain> db_main, const std::string &db_name, const std::string &table_root,
                   const std::vector<std::string> &queries) {
  // cache db main and members
  db_main_ = db_main;
  txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
  block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
  catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
  txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

  auto txn = txn_manager_->BeginTransaction();

  // Create database catalog and namespace
  db_oid_ = catalog_->CreateDatabase(txn, db_name, true);
  auto accessor = catalog_->GetAccessor(txn, db_oid_);
  ns_oid_ = accessor->GetDefaultNamespace();

  // Make the execution context
  execution::exec::ExecutionContext exec_ctx{db_oid_, txn, nullptr, nullptr, std::move(accessor)};

  // create the TPCH database and compile the queries
  GenerateTPCHTables(&exec_ctx, table_root);
  LoadTPCHQueries(&exec_ctx, queries);

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

void Workload::GenerateTPCHTables(execution::exec::ExecutionContext *exec_ctx, const std::string &dir_name) {
  // TPCH table names;
  static const std::vector<std::string> tpch_tables{
      "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
  };
  execution::sql::TableReader table_reader(exec_ctx, block_store_.Get(), ns_oid_);
  for (const auto &table_name : tpch_tables) {
    auto num_rows = table_reader.ReadTable(dir_name + table_name + ".schema", dir_name + table_name + ".data");
    EXECUTION_LOG_INFO("Wrote {} rows on table {}.", num_rows, table_name);
  }
}

void Workload::LoadTPCHQueries(execution::exec::ExecutionContext *exec_ctx, const std::vector<std::string> &queries) {
  for (auto &query_file : queries) {
    queries_.emplace_back(
        execution::ExecutableQuery(query_file, common::ManagedPointer<execution::exec::ExecutionContext>(exec_ctx)));
  }
}

void Workload::Execute(int8_t worker_id, uint32_t num_precomputed_txns_per_worker, execution::vm::ExecutionMode mode) {
  // Shuffle the queries randomly for each thread
  auto num_queries = queries_.size();
  uint32_t index[num_queries];
  std::shuffle(&index[0], &index[num_queries], std::mt19937(worker_id));

  // Register to the metrics manager
  db_main_->GetMetricsManager()->RegisterThread();
  uint32_t counter = 0;
  for (uint32_t i = 0; i < num_precomputed_txns_per_worker; i++) {
    // Executing all the queries on by one in round robin
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(txn, db_oid_);
    execution::exec::ExecutionContext exec_ctx{db_oid_, txn, nullptr, nullptr, std::move(accessor)};
    queries_[index[counter]].Run(common::ManagedPointer<execution::exec::ExecutionContext>(&exec_ctx), mode);
    counter = counter == num_queries - 1?0:counter + 1 ;
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  // Unregister from the metrics manager
  db_main_->GetMetricsManager()->UnregisterThread();
}

}  // namespace terrier::tpch
