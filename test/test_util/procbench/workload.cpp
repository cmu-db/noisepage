#include "test_util/procbench/workload.h"

#include <array>
#include <random>
#include <string>

#include "common/managed_pointer.h"
#include "execution/compiler/output_schema_util.h"
#include "execution/exec/execution_context_builder.h"
#include "execution/sql/value_util.h"
#include "execution/table_generator/table_generator.h"
#include "main/db_main.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "test_util/procbench/procbench_query.h"

namespace noisepage::procbench {

/** ProcBench table names */
static const std::vector<std::string> PROCBENCH_TABLE_NAMES{"call_center",
                                                            "catalog_page",
                                                            "catalog_returns_history",
                                                            "catalog_returns",
                                                            "catalog_sales_history",
                                                            "catalog_sales",
                                                            "customer_address",
                                                            "customer_demographics",
                                                            "customer",
                                                            "date_dim",
                                                            "household_demographics",
                                                            "income_band",
                                                            "inventory_history",
                                                            "inventory",
                                                            "item",
                                                            "promotion",
                                                            "reason",
                                                            "ship_mode",
                                                            "store_returns_history",
                                                            "store_returns",
                                                            "store_sales_history",
                                                            "store_sales",
                                                            "store",
                                                            "time_dim",
                                                            "warehouse",
                                                            "web_page",
                                                            "web_returns_history",
                                                            "web_returns",
                                                            "web_sales_history",
                                                            "web_sales",
                                                            "web_site"};

Workload::Workload(common::ManagedPointer<DBMain> db_main, const std::string &db_name, const std::string &table_root) {
  // cache db main and members
  db_main_ = db_main;
  txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
  block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
  catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
  txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

  auto txn = txn_manager_->BeginTransaction();

  // Create database catalog and namespace
  db_oid_ = catalog_->CreateDatabase(common::ManagedPointer<transaction::TransactionContext>(txn), db_name, true);
  auto accessor =
      catalog_->GetAccessor(common::ManagedPointer<transaction::TransactionContext>(txn), db_oid_, DISABLED);
  ns_oid_ = accessor->GetDefaultNamespace();

  // Enable counters and disable the parallel execution for this workload
  exec_settings_.is_parallel_execution_enabled_ = false;
  exec_settings_.is_counters_enabled_ = true;

  // Make the execution context
  auto exec_ctx = execution::exec::ExecutionContextBuilder()
                      .WithDatabaseOID(db_oid_)
                      .WithExecutionSettings(exec_settings_)
                      .WithTxnContext(common::ManagedPointer{txn})
                      .WithOutputSchema(execution::exec::ExecutionContext::NULL_OUTPUT_SCHEMA)
                      .WithOutputCallback(execution::exec::ExecutionContext::NULL_OUTPUT_CALLBACK)
                      .WithCatalogAccessor(common::ManagedPointer{accessor})
                      .WithMetricsManager(db_main->GetMetricsManager())
                      .WithReplicationManager(DISABLED)
                      .WithRecoveryManager(DISABLED)
                      .Build();

  // Create the ProcBench database
  LoadTables(exec_ctx.get(), table_root);
  // Compile all queries for the benchmark
  LoadQueries(accessor);

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

void Workload::LoadTables(execution::exec::ExecutionContext *exec_ctx, const std::string &directory) {
  EXECUTION_LOG_INFO("Loading tables for ProcBench benchmark...");
  execution::sql::TableReader table_reader{exec_ctx, block_store_.Get(), ns_oid_};
  for (const auto &table_name : PROCBENCH_TABLE_NAMES) {
    const std::string data_path = fmt::format("{}{}.data", directory, table_name);
    const std::string schema_path = fmt::format("{}{}.schema", directory, table_name);
    const auto num_rows = table_reader.ReadTable(schema_path, data_path);
    EXECUTION_LOG_INFO("Wrote {} rows on table {}.", num_rows, table_name);
  }
  EXECUTION_LOG_INFO("Done.");
}

void Workload::LoadQueries(const std::unique_ptr<catalog::CatalogAccessor> &accessor) {
  EXECUTION_LOG_INFO("Loading queries for ProcBench benchmark...");
  // Executable query and plan node are stored as a tuple as the entry of vector
  (void)accessor;
  // query_and_plan_.emplace_back(TPCHQuery::MakeExecutableQ1(accessor, exec_settings_));
  EXECUTION_LOG_INFO("Done.");
}

void Workload::Execute(int8_t worker_id, uint64_t execution_us_per_worker, uint64_t avg_interval_us, uint32_t query_id,
                       execution::vm::ExecutionMode mode) {
  // Shuffle the queries randomly for each thread
  const auto total_query_num = query_and_plan_.size();
  std::vector<uint32_t> index{};
  index.resize(total_query_num);
  std::iota(index.begin(), index.end(), 0);
  std::shuffle(index.begin(), index.end(), std::mt19937(time(nullptr) + worker_id));

  // Get the sleep time range distribution
  std::mt19937 generator{};
  std::uniform_int_distribution<uint64_t> distribution(avg_interval_us - avg_interval_us / 2,
                                                       avg_interval_us + avg_interval_us / 2);

  // Register to the metrics manager
  db_main_->GetMetricsManager()->RegisterThread();
  uint32_t counter = 0;
  uint64_t end_time = metrics::MetricsUtil::Now() + execution_us_per_worker;
  while (metrics::MetricsUtil::Now() < end_time) {
    // Executing all the queries on by one in round robin
    auto txn = txn_manager_->BeginTransaction();
    auto accessor =
        catalog_->GetAccessor(common::ManagedPointer<transaction::TransactionContext>(txn), db_oid_, DISABLED);

    auto output_schema = std::get<1>(query_and_plan_[index[counter]])->GetOutputSchema().Get();
    // Uncomment this line and change output.cpp:90 to EXECUTION_LOG_INFO to print output
    // execution::exec::OutputPrinter printer(output_schema);
    execution::exec::NoOpResultConsumer printer;

    auto exec_ctx = execution::exec::ExecutionContextBuilder()
                        .WithDatabaseOID(db_oid_)
                        .WithExecutionSettings(exec_settings_)
                        .WithTxnContext(common::ManagedPointer{txn})
                        .WithOutputSchema(common::ManagedPointer{output_schema})
                        .WithOutputCallback(printer)
                        .WithCatalogAccessor(common::ManagedPointer{accessor})
                        .WithMetricsManager(db_main_->GetMetricsManager())
                        .WithReplicationManager(DISABLED)
                        .WithRecoveryManager(DISABLED)
                        .Build();

    std::get<0>(query_and_plan_[index[counter]])
        ->Run(common::ManagedPointer<execution::exec::ExecutionContext>(exec_ctx), mode);

    // Only execute up to query_num number of queries for this thread in round-robin
    counter = counter == query_id - 1 ? 0 : counter + 1;
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Sleep to create different execution frequency patterns
    auto random_sleep_time = distribution(generator);
    std::this_thread::sleep_for(std::chrono::microseconds(random_sleep_time));
  }

  // Unregister from the metrics manager
  db_main_->GetMetricsManager()->UnregisterThread();
}

}  // namespace noisepage::procbench
