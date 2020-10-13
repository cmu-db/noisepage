#include "test_util/tpch/workload.h"

#include <random>
#include <string>

#include "common/managed_pointer.h"
#include "execution/compiler/output_schema_util.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/value_util.h"
#include "execution/table_generator/table_generator.h"
#include "main/db_main.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "test_util/ssb/star_schema_query.h"
#include "test_util/tpch/tpch_query.h"

namespace terrier::tpch {

Workload::Workload(common::ManagedPointer<DBMain> db_main, const std::string &db_name, const std::string &table_root,
                   enum BenchmarkType type) {
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

  // Make the execution context
  auto exec_ctx = execution::exec::ExecutionContext(
      db_oid_, common::ManagedPointer<transaction::TransactionContext>(txn), nullptr, nullptr,
      common::ManagedPointer<catalog::CatalogAccessor>(accessor), exec_settings_);

  // create the TPCH database and compile the queries
  GenerateTables(&exec_ctx, table_root, type);
  LoadQueries(accessor, type);

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

void Workload::GenerateTables(execution::exec::ExecutionContext *exec_ctx, const std::string &dir_name,
                              enum BenchmarkType type) {
  // TPCH table names;
  static const std::vector<std::string> tpch_tables{"part",   "supplier", "partsupp", "customer",
                                                    "orders", "lineitem", "nation",   "region"};

  // SSB table names;
  static const std::vector<std::string> ssb_tables{"part", "lineorder", "customer", "date", "supplier"};

  const std::vector<std::string> *tables;
  std::string kind;
  switch (type) {
    case tpch::Workload::BenchmarkType::TPCH:
      tables = &tpch_tables;
      kind = ".data";
      break;
    case tpch::Workload::BenchmarkType::SSB:
      tables = &ssb_tables;
      kind = ".csv";
      break;
    default:
      UNREACHABLE("unimplemented benchmark type");
  }
  execution::sql::TableReader table_reader(exec_ctx, block_store_.Get(), ns_oid_);

  for (const auto &table_name : *tables) {
    auto table_dir = dir_name + table_name;
    auto num_rows = table_reader.ReadTable(dir_name + table_name + ".schema", table_dir.append(kind));
    EXECUTION_LOG_INFO("Wrote {} rows on table {}.", num_rows, table_name);
  }
}

void Workload::LoadQueries(const std::unique_ptr<catalog::CatalogAccessor> &accessor, enum BenchmarkType type) {
  // Executable query and plan node are stored as a tuple as the entry of vector
  // TODO(wuwenw): add q16 after LIKE PR get merged
  switch (type) {
    case tpch::Workload::BenchmarkType::TPCH:
      query_and_plan_.emplace_back(TPCHQuery::MakeExecutableQ1(accessor, exec_settings_));
      query_and_plan_.emplace_back(TPCHQuery::MakeExecutableQ4(accessor, exec_settings_));
      query_and_plan_.emplace_back(TPCHQuery::MakeExecutableQ5(accessor, exec_settings_));
      query_and_plan_.emplace_back(TPCHQuery::MakeExecutableQ6(accessor, exec_settings_));
      query_and_plan_.emplace_back(TPCHQuery::MakeExecutableQ7(accessor, exec_settings_));
      query_and_plan_.emplace_back(TPCHQuery::MakeExecutableQ11(accessor, exec_settings_));
      query_and_plan_.emplace_back(TPCHQuery::MakeExecutableQ16(accessor, exec_settings_));
      query_and_plan_.emplace_back(TPCHQuery::MakeExecutableQ18(accessor, exec_settings_));
      query_and_plan_.emplace_back(TPCHQuery::MakeExecutableQ19(accessor, exec_settings_));
      break;
    case tpch::Workload::BenchmarkType::SSB:
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ1Part1(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ1Part2(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ1Part3(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ2Part1(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ2Part2(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ2Part3(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ3Part1(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ3Part2(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ3Part3(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ3Part4(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ4Part1(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ4Part2(accessor, exec_settings_));
      query_and_plan_.emplace_back(ssb::SSBQuery::SSBMakeExecutableQ4Part3(accessor, exec_settings_));
      break;
    default:
      UNREACHABLE("Unimplemented Benchmark Type");
  }
}

void Workload::Execute(int8_t worker_id, uint64_t execution_us_per_worker, uint64_t avg_interval_us, uint32_t query_num,
                       execution::vm::ExecutionMode mode) {
  // Shuffle the queries randomly for each thread
  auto total_query_num = query_and_plan_.size();
  std::vector<uint32_t> index;
  index.resize(total_query_num);
  for (uint32_t i = 0; i < total_query_num; ++i) index[i] = i;
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
    auto exec_ctx = execution::exec::ExecutionContext(
        db_oid_, common::ManagedPointer<transaction::TransactionContext>(txn), printer, output_schema,
        common::ManagedPointer<catalog::CatalogAccessor>(accessor), exec_settings_);

    std::get<0>(query_and_plan_[index[counter]])
        ->Run(common::ManagedPointer<execution::exec::ExecutionContext>(&exec_ctx), mode);

    // Only execute up to query_num number of queries for this thread in round-robin
    counter = counter == query_num - 1 ? 0 : counter + 1;
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Sleep to create different execution frequency patterns
    auto random_sleep_time = distribution(generator);
    std::this_thread::sleep_for(std::chrono::microseconds(random_sleep_time));
  }

  // Unregister from the metrics manager
  db_main_->GetMetricsManager()->UnregisterThread();
}

}  // namespace terrier::tpch
