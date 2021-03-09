#include "test_util/tpcc/workload_cached.h"

#include <random>
#include <string>

#include "binder/bind_node_visitor.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_context.h"
#include "main/db_main.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "test_util/tpcc/builder.h"
#include "test_util/tpcc/database.h"
#include "test_util/tpcc/loader.h"
#include "test_util/tpcc/worker.h"
#include "traffic_cop/traffic_cop_util.h"

namespace noisepage::tpcc {

WorkloadCached::WorkloadCached(common::ManagedPointer<DBMain> db_main, const std::vector<std::string> &txn_names,
                               int8_t num_threads) {
  // cache db main and members
  db_main_ = db_main;
  txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
  block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
  catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
  txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

  // build the TPCC database using HashMaps where possible
  Builder tpcc_builder{block_store_, catalog_, txn_manager_};
  tpcc_db_ = tpcc_builder.Build(storage::index::IndexType::HASHMAP);
  db_oid_ = tpcc_db_->db_oid_;

  GenerateTPCCTables(num_threads);

  // initialize and compile the queries
  InitializeSQLs();
  LoadTPCCQueries(txn_names);
}

void WorkloadCached::GenerateTPCCTables(int8_t num_threads) {
  // populate the tables and indexes
  // prepare the thread pool and workers
  common::WorkerPool thread_pool{static_cast<uint32_t>(num_threads), {}};
  std::vector<Worker> workers;
  workers.reserve(num_threads);
  workers.clear();
  for (int8_t i = 0; i < num_threads; i++) {
    workers.emplace_back(tpcc_db_);
  }
  thread_pool.Startup();
  Loader::PopulateDatabase(txn_manager_, tpcc_db_, &workers, &thread_pool);
}

void WorkloadCached::LoadTPCCQueries(const std::vector<std::string> &txn_names) {
  uint64_t optimizer_timeout =
      static_cast<uint64_t>(db_main_->GetSettingsManager()->GetInt(settings::Param::task_execution_timeout));

  for (auto &txn_name : txn_names) {
    // read queries from files
    std::vector<std::unique_ptr<execution::compiler::ExecutableQuery>> exec_queries;

    for (auto &query : sqls_.find(txn_name)->second) {
      const auto parse_result = parser::PostgresParser::BuildParseTree(query);
      transaction::TransactionContext *txn = txn_manager_->BeginTransaction();

      auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid_, DISABLED);
      binder::BindNodeVisitor visitor(common::ManagedPointer(accessor.get()), db_oid_);
      visitor.BindNameToNode(common::ManagedPointer<parser::ParseResult>(parse_result), nullptr, nullptr);

      // generate plan node
      std::unique_ptr<planner::AbstractPlanNode> plan_node =
          trafficcop::TrafficCopUtil::Optimize(
              common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(parse_result),
              db_oid_, db_main_->GetStatsStorage(), std::make_unique<optimizer::TrivialCostModel>(), optimizer_timeout,
              nullptr)
              ->TakePlanNodeOwnership();

      auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
          db_oid_, common::ManagedPointer(txn), nullptr, nullptr, common::ManagedPointer(accessor), exec_settings_,
          db_main_->GetMetricsManager(), DISABLED);

      // generate executable query and emplace it into the vector; break down here
      auto exec_query =
          std::make_unique<execution::compiler::ExecutableQuery>(*plan_node, exec_ctx->GetExecutionSettings());
      exec_queries.emplace_back(std::move(exec_query));

      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    }

    queries_.emplace(txn_name, std::move(exec_queries));

    // record the name of transaction
    txn_names_.emplace_back(txn_name);
  }
}

void WorkloadCached::Execute(int8_t worker_id, uint32_t num_precomputed_txns_per_worker,
                             execution::vm::ExecutionMode mode) {
  // Shuffle the queries randomly for each thread
  uint32_t num_queries = queries_.size();
  uint32_t index[num_queries];
  for (uint32_t i = 0; i < num_queries; ++i) index[i] = i;
  std::shuffle(&index[0], &index[num_queries], std::mt19937(worker_id));

  // Register to the metrics manager
  db_main_->GetMetricsManager()->RegisterThread();
  uint32_t counter = 0;
  for (uint32_t i = 0; i < num_precomputed_txns_per_worker; i++) {
    // Executing all the queries on by one in round robin
    auto txn = txn_manager_->BeginTransaction();
    auto accessor =
        catalog_->GetAccessor(common::ManagedPointer<transaction::TransactionContext>(txn), db_oid_, DISABLED);
    for (const auto &query : queries_.find(txn_names_[index[counter]])->second) {
      execution::exec::ExecutionContext exec_ctx{db_oid_,
                                                 common::ManagedPointer<transaction::TransactionContext>(txn),
                                                 nullptr,
                                                 nullptr,  // FIXME: Get the correct output later
                                                 common::ManagedPointer<catalog::CatalogAccessor>(accessor),
                                                 exec_settings_,
                                                 db_main_->GetMetricsManager(),
                                                 DISABLED};
      query->Run(common::ManagedPointer<execution::exec::ExecutionContext>(&exec_ctx), mode);
    }
    counter = counter == num_queries - 1 ? 0 : counter + 1;
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  // Unregister from the metrics manager
  db_main_->GetMetricsManager()->UnregisterThread();
}

void WorkloadCached::InitDelivery() {
  auto curr = sqls_.emplace("delivery", std::vector<std::string>{});
  std::vector<std::string> &vec = curr.first->second;
  vec.emplace_back("SELECT NO_O_ID FROM \"NEW ORDER\" WHERE NO_D_ID = 1 AND NO_W_ID = 2 ORDER BY NO_O_ID LIMIT 1");
  vec.emplace_back("DELETE FROM \"NEW ORDER\" WHERE NO_O_ID = 1 AND NO_D_ID = 2 AND NO_W_ID = 3");
  vec.emplace_back("SELECT O_C_ID FROM \"ORDER\" WHERE O_ID = 1 AND (O_D_ID = 2 AND O_W_ID = 3)");
  vec.emplace_back("UPDATE \"ORDER\" SET O_CARRIER_ID = 1 WHERE O_ID = 1 AND O_D_ID = 2 AND O_W_ID = 3");
  vec.emplace_back("UPDATE \"ORDER LINE\" SET OL_DELIVERY_D = 1 WHERE OL_O_ID = 1 AND OL_D_ID = 2 AND OL_W_ID = 3");
  vec.emplace_back("SELECT SUM(OL_AMOUNT) AS OL_TOTAL FROM \"ORDER LINE\" WHERE OL_O_ID=1 AND OL_D_ID=2 AND OL_W_ID=3");
  vec.emplace_back(
      "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE+1, C_DELIVERY_CNT=C_DELIVERY_CNT+1 WHERE C_W_ID=1 AND C_D_ID=2 AND "
      "C_ID=3");
}

void WorkloadCached::InitIndexScan() {
  auto curr = sqls_.emplace("index_scan", std::vector<std::string>{});
  std::vector<std::string> &vec = curr.first->second;
  vec.emplace_back("SELECT NO_O_ID FROM \"NEW ORDER\" WHERE NO_D_ID = 1");
  vec.emplace_back("SELECT NO_O_ID FROM \"NEW ORDER\" WHERE 1 < NO_D_ID");
  vec.emplace_back("SELECT NO_O_ID FROM \"NEW ORDER\" ORDER BY NO_W_ID");
  vec.emplace_back("SELECT NO_O_ID FROM \"NEW ORDER\" ORDER BY NO_W_ID DESC");
  vec.emplace_back("SELECT NO_O_ID FROM \"NEW ORDER\" ORDER BY NO_D_ID");
  vec.emplace_back("SELECT NO_O_ID FROM \"NEW ORDER\" ORDER BY NO_W_ID, NO_D_ID DESC");
  vec.emplace_back("SELECT NO_O_ID FROM \"NEW ORDER\" WHERE NO_W_ID = 1 ORDER BY NO_W_ID");
  vec.emplace_back("SELECT NO_O_ID FROM \"NEW ORDER\" WHERE NO_W_ID = 1 ORDER BY NO_W_ID LIMIT 15 OFFSET 445");
}

void WorkloadCached::InitNewOrder() {
  auto curr = sqls_.emplace("neworder", std::vector<std::string>{});
  std::vector<std::string> &vec = curr.first->second;
  vec.emplace_back("SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID=1 AND C_D_ID=2 AND C_ID=3");
  vec.emplace_back("SELECT W_TAX FROM WAREHOUSE WHERE W_ID=1");
  vec.emplace_back("SELECT D_NEXT_O_ID, D_TAX FROM DISTRICT WHERE D_W_ID=1 AND D_ID=2");
  vec.emplace_back("INSERT INTO \"NEW ORDER\" (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (1,2,3)");
  vec.emplace_back("UPDATE DISTRICT SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = 1 AND D_ID = 2");
  vec.emplace_back(
      "INSERT INTO \"ORDER\" (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) VALUES (1,2,3,4,0,6,7)");
  vec.emplace_back("SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID=1");
  vec.emplace_back(
      "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, "
      "S_DIST_08, S_DIST_09, S_DIST_10 FROM STOCK WHERE S_I_ID=1 AND S_W_ID=2");
  vec.emplace_back(
      "UPDATE STOCK SET S_QUANTITY = 1, S_YTD = S_YTD + 1, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT ="
      "S_REMOTE_CNT + 1 WHERE S_I_ID = 2 AND S_W_ID = 3");
}

void WorkloadCached::InitOrderStatus() {
  auto curr = sqls_.emplace("orderstatus", std::vector<std::string>{});
  std::vector<std::string> &vec = curr.first->second;
  vec.emplace_back(
      "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM \"ORDER\" WHERE O_W_ID=1 AND O_D_ID=2 AND O_C_ID=3 ORDER BY O_ID DESC "
      "LIMIT 1");
  vec.emplace_back(
      "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM \"ORDER LINE\" WHERE OL_O_ID=1 AND "
      "OL_D_ID=2 AND OL_W_ID=3");
  vec.emplace_back(
      "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, "
      "C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM CUSTOMER WHERE C_W_ID=1 AND "
      "C_D_ID=2 AND C_ID=3");
  vec.emplace_back(
      "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, "
      "C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM CUSTOMER WHERE C_W_ID=1 AND "
      "C_D_ID=2 AND C_LAST='page' ORDER BY C_FIRST");
}

void WorkloadCached::InitPayment() {
  auto curr = sqls_.emplace("payment", std::vector<std::string>{});
  std::vector<std::string> &vec = curr.first->second;
  vec.emplace_back("UPDATE WAREHOUSE SET W_YTD = W_YTD + 1 WHERE W_ID = 2");
  vec.emplace_back("SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME FROM WAREHOUSE WHERE W_ID=1");
  vec.emplace_back("UPDATE DISTRICT SET D_YTD = D_YTD + 1 WHERE D_W_ID = 2 AND D_ID = 3");
  vec.emplace_back(
      "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME FROM DISTRICT WHERE D_W_ID=1 AND D_ID=2");
  vec.emplace_back(
      "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, "
      "C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM CUSTOMER WHERE C_W_ID=1 AND "
      "C_D_ID=2 AND C_ID=3");
  vec.emplace_back("SELECT C_DATA FROM CUSTOMER WHERE C_W_ID=1 AND C_D_ID=2 AND C_ID=3");
  vec.emplace_back(
      "UPDATE CUSTOMER SET C_BALANCE = 1, C_YTD_PAYMENT = 2, C_PAYMENT_CNT = 3, C_DATA = '4' WHERE C_W_ID = 1 AND "
      "C_D_ID = 2 AND C_ID = 3");
  vec.emplace_back(
      "INSERT INTO HISTORY (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) VALUES "
      "(1,2,3,4,5,0,7,'data')");
  vec.emplace_back(
      "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, "
      "C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM CUSTOMER WHERE C_W_ID=1 AND "
      "C_D_ID=2 AND C_LAST='page' ORDER BY C_FIRST");
}

void WorkloadCached::InitSeqScan() {
  auto curr = sqls_.emplace("seq_scan", std::vector<std::string>{});
  std::vector<std::string> &vec = curr.first->second;
  vec.emplace_back("SELECT NO_O_ID FROM \"NEW ORDER\"");
  vec.emplace_back("SELECT o_id from \"ORDER\" where o_carrier_id = 5");
  vec.emplace_back("SELECT o_id from \"ORDER\" where o_carrier_id = 5 ORDER BY o_ol_cnt DESC");
  vec.emplace_back("SELECT o_id from \"ORDER\" where o_carrier_id = 5 LIMIT 1 OFFSET 2");
  vec.emplace_back("SELECT o_id from \"ORDER\" where o_carrier_id = 5 ORDER BY o_ol_cnt DESC LIMIT 1 OFFSET 2");
}

void WorkloadCached::InitStockLevel() {
  auto curr = sqls_.emplace("stocklevel", std::vector<std::string>{});
  std::vector<std::string> &vec = curr.first->second;
  vec.emplace_back("SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID=1 AND D_ID=2");
  vec.emplace_back(
      "SELECT COUNT(DISTINCT S_I_ID) AS STOCK_COUNT FROM \"ORDER LINE\", STOCK WHERE OL_W_ID = 1 AND OL_D_ID = 2 AND "
      "OL_O_ID < 3 AND OL_O_ID >= 4 AND S_W_ID = 5 AND S_I_ID = OL_I_ID AND S_QUANTITY < 6");
  vec.emplace_back("SELECT S_I_ID FROM \"ORDER LINE\", STOCK WHERE OL_W_ID=1 AND S_W_ID=1 AND OL_W_ID=S_W_ID");
}

void WorkloadCached::InitializeSQLs() {
  // "delivery", "index_scan", "neworder", "orderstatus", "payment", "seq_scan", "stocklevel"
  InitDelivery();
  InitIndexScan();
  InitNewOrder();
  InitOrderStatus();
  InitPayment();
  InitSeqScan();
  InitStockLevel();
}
}  // namespace noisepage::tpcc
