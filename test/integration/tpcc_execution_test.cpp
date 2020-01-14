#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
#include "execution/execution_util.h"
#include "main/db_main.h"
#include "parser/postgresparser.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/builder.h"
#include "test_util/tpcc/database.h"
#include "test_util/tpcc/loader.h"
#include "test_util/tpcc/worker.h"
#include "traffic_cop/traffic_cop_util.h"
#include "transaction/transaction_util.h"

namespace terrier {

class TpccExecutionTest : public TerrierTest {
  void SetUp() override {
    db_main_ = terrier::DBMain::Builder()
                   .SetUseGC(true)
                   .SetUseGCThread(true)
                   .SetUseCatalog(true)
                   .SetUseStatsStorage(true)
                   .SetUseExecution(true)
                   .Build();

    stats_storage_ = db_main_->GetStatsStorage();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

    SetUpTPCC();
  }

  void TearDown() override {
    tpcc::Util::UnregisterIndexesForGC(db_main_->GetStorageLayer()->GetGarbageCollector(),
                                       common::ManagedPointer(tpcc_db_));
  }

  std::unique_ptr<DBMain> db_main_;

 public:
  std::unique_ptr<tpcc::Database> tpcc_db_;
  common::ManagedPointer<optimizer::StatsStorage> stats_storage_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  static constexpr std::string_view DB_NAME = "tpcc";

  const uint8_t num_threads_ = 1;
  const uint64_t optimizer_timeout_ = 5000;

 private:
  void SetUpTPCC() {
    tpcc::Builder tpcc_builder(db_main_->GetStorageLayer()->GetBlockStore(), catalog_, txn_manager_);
    tpcc_db_.reset(tpcc_builder.Build(storage::index::IndexType::HASHMAP));

    tpcc::Util::RegisterIndexesForGC(db_main_->GetStorageLayer()->GetGarbageCollector(),
                                     common::ManagedPointer(tpcc_db_));

    // Workers and thread pool are only used for parallel population of the database
    //    std::vector<tpcc::Worker> workers;
    //    workers.reserve(num_threads_);
    //    for (int8_t i = 0; i < num_threads_; i++) {
    //      workers.emplace_back(tpcc_db_.get());
    //    }
    //    common::WorkerPool thread_pool{num_threads_, {}};

    //    tpcc::Loader::PopulateDatabase(txn_manager_, tpcc_db_.get(), &workers, &thread_pool);
  }
};

// NOLINTNEXTLINE
TEST_F(TpccExecutionTest, SimpleTest) {
  //  const std::string query_string = "INSERT INTO HISTORY VALUES (1,1,1,1,1,1,1.0,'A')";
  const std::string query_string = "INSERT INTO \"NEW ORDER\" VALUES (1,2,3)";
  //  const std::string query_string = "SELECT * FROM \"NEW ORDER\"";

  auto *const txn = txn_manager_->BeginTransaction();
  const auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), tpcc_db_->db_oid_);

  auto query = trafficcop::TrafficCopUtil::Parse(query_string);
  trafficcop::TrafficCopUtil::Bind(common::ManagedPointer(accessor), std::string(DB_NAME),
                                   common::ManagedPointer(query));

  auto physical_plan =
      trafficcop::TrafficCopUtil::Optimize(common::ManagedPointer(txn), common::ManagedPointer(accessor),
                                           common::ManagedPointer(query), stats_storage_, optimizer_timeout_);

  execution::exec::OutputPrinter printer{physical_plan->GetOutputSchema().Get()};

  //  std::cout << physical_plan->ToJson() << std::endl;

  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(tpcc_db_->db_oid_, common::ManagedPointer(txn),
                                                                      printer, physical_plan->GetOutputSchema().Get(),
                                                                      common::ManagedPointer(accessor));

  auto exec_query = execution::ExecutableQuery(common::ManagedPointer(physical_plan), common::ManagedPointer(exec_ctx));

  exec_query.Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

}  // namespace terrier