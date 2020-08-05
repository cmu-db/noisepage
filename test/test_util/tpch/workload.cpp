#include "test_util/tpch/workload.h"

#include <random>

#include "common/managed_pointer.h"
#include "execution/exec/execution_context.h"
#include "execution/execution_util.h"
//#include "test_util/tpch/tpch_query.h"
#include "execution/sql/value_util.h"
#include "execution/table_generator/table_generator.h"
#include "main/db_main.h"

namespace terrier::tpch {

Workload::Workload(common::ManagedPointer<DBMain> db_main, const std::string &db_name, const std::string &table_root,
                   transaction::TransactionContext *txn, execution::exec::ExecutionContext *exec_ctx) {
  // cache db main and members
  db_main_ = db_main;
  txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
  block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
  catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
  txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
  ns_oid_ = exec_ctx->GetAccessor()->GetDefaultNamespace();

  // create the TPCH database and compile the queries
  GenerateTPCHTables(exec_ctx, table_root);

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
  //tpch::TPCHQuery();
}

//void Workload::LoadTPCHQueries(execution::exec::ExecutionContext *exec_ctx) {
//  tpch::TPCHQuery();
//  queries_.emplace_back(execution::compiler::ExecutableQuery(
//      query_file, common::ManagedPointer<execution::exec::ExecutionContext>(exec_ctx), true));
//}
}  // namespace terrier::tpch
