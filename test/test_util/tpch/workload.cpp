#include "test_util/tpch/workload.h"

#include <random>

#include "common/managed_pointer.h"
#include "execution/exec/execution_context.h"
#include "execution/execution_util.h"
#include "execution/sql/value_util.h"
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
  db_oid_ = catalog_->CreateDatabase(common::ManagedPointer<transaction::TransactionContext>(txn), db_name, true);
  auto accessor = catalog_->GetAccessor(common::ManagedPointer<transaction::TransactionContext>(txn), db_oid_);
  ns_oid_ = accessor->GetDefaultNamespace();

  // Make the execution context
  execution::exec::ExecutionContext exec_ctx{db_oid_, common::ManagedPointer<transaction::TransactionContext>(txn),
                                             nullptr, nullptr,
                                             common::ManagedPointer<catalog::CatalogAccessor>(accessor)};

  // create the TPCH database and compile the queries
  GenerateTPCHTables(&exec_ctx, table_root);
  LoadTPCHQueries(&exec_ctx, queries);

  // Initialize the TPCH outputs
  sample_output_.InitTestOutput();

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
    queries_.emplace_back(execution::ExecutableQuery(
        query_file, common::ManagedPointer<execution::exec::ExecutionContext>(exec_ctx), true));
  }
}

std::vector<parser::ConstantValueExpression> Workload::GetQueryParams(const std::string &query_name) {
  std::vector<parser::ConstantValueExpression> params;
  params.reserve(8);

  // Add the identifier for each pipeline. At most 8 query pipelines for now
  for (int i = 0; i < 8; ++i) {
    const std::string query_val = query_name + "_p" + std::to_string(i + 1);

    auto string_val = execution::sql::ValueUtil::CreateStringVal(query_val);
    params.emplace_back(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));
  }

  return params;
}

void Workload::Execute(int8_t worker_id, uint64_t execution_us_per_worker, uint64_t avg_interval_us, uint32_t query_num,
                       execution::vm::ExecutionMode mode) {
  // Shuffle the queries randomly for each thread
  auto total_query_num = queries_.size();
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
    auto accessor = catalog_->GetAccessor(common::ManagedPointer<transaction::TransactionContext>(txn), db_oid_);
    execution::ExecutableQuery &query = queries_[index[counter]];
    auto &query_name = query.GetQueryName();
    auto output_schema = sample_output_.GetSchema(query_name);
    execution::exec::NoOpResultConsumer printer;
    // execution::exec::OutputPrinter printer(output_schema);
    execution::exec::ExecutionContext exec_ctx{db_oid_, common::ManagedPointer<transaction::TransactionContext>(txn),
                                               printer, output_schema,
                                               common::ManagedPointer<catalog::CatalogAccessor>(accessor)};
    const auto params = GetQueryParams(query_name);
    exec_ctx.SetParams(common::ManagedPointer(&params));
    query.Run(common::ManagedPointer<execution::exec::ExecutionContext>(&exec_ctx), mode);
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
