#include <memory>
#include <stack>
#include <utility>
#include <vector>

#include "execution/sql/value.h"
#include "execution/compiler/output_checker.h"
#include "execution/vm/module.h"
#include "execution/exec/execution_context.h"
#include "execution/executable_query.h"
#include "main/db_main.h"
#include "network/connection_context.h"
#include "network/postgres/statement.h"
#include "network/postgres/portal.h"
#include "network/network_io_utils.h"
#include "network/postgres/postgres_packet_writer.h"
#include "traffic_cop/traffic_cop_defs.h"
#include "parser/postgresparser.h"
#include "optimizer/binding.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/optimizer_task.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/pattern.h"
#include "optimizer/optimizer.h"
#include "optimizer/cost_model/index_fit_cost_model.h"

#include "test_util/test_harness.h"

namespace terrier::optimizer {

struct IdxJoinTest : public TerrierTest {
  const uint64_t optimizer_timeout_ = 1000000;

  void ExecuteSQL(std::string sql, network::QueryType qtype) {
    std::vector<type::TransientValue> params;
    tcop_->BeginTransaction(common::ManagedPointer(&context_));
    auto parse = tcop_->ParseQuery(sql, common::ManagedPointer(&context_));
    auto stmt = network::Statement(std::move(parse));
    auto result = tcop_->BindQuery(common::ManagedPointer(&context_), common::ManagedPointer(&stmt), common::ManagedPointer(&params));
    TERRIER_ASSERT(result.type_ == trafficcop::ResultType::COMPLETE, "Bind should have succeeded");

    auto plan = tcop_->OptimizeBoundQuery(common::ManagedPointer(&context_), stmt.ParseResult());
    if (qtype >= network::QueryType::QUERY_CREATE_TABLE) {
      result = tcop_->ExecuteCreateStatement(common::ManagedPointer(&context_), common::ManagedPointer(plan), qtype);
      TERRIER_ASSERT(result.type_ == trafficcop::ResultType::COMPLETE, "Execute should have succeeded");
    } else {
      network::WriteQueue queue;
      auto pwriter = network::PostgresPacketWriter(common::ManagedPointer(&queue));
      auto portal = network::Portal(common::ManagedPointer(&stmt), std::move(plan));
      result = tcop_->CodegenAndRunPhysicalPlan(common::ManagedPointer(&context_),
                                                common::ManagedPointer(&pwriter),
                                                common::ManagedPointer(&portal));
      TERRIER_ASSERT(result.type_ == trafficcop::ResultType::COMPLETE, "Execute should have succeeded");
    }

    tcop_->EndTransaction(common::ManagedPointer(&context_), network::QueryType::QUERY_COMMIT);
  }

  void SetUp() override { 
    TerrierTest::SetUp();

    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    settings::SettingsManager::ConstructParamMap(param_map);

    db_main_ = terrier::DBMain::Builder()
      .SetUseGC(true)
      .SetSettingsParameterMap(std::move(param_map))
      .SetUseSettingsManager(true)
      .SetUseCatalog(true)
      .SetUseStatsStorage(true)
      .SetUseTrafficCop(true)
      .SetUseExecution(true)
      .Build();

    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

    tcop_ = db_main_->GetTrafficCop();
    auto oids = tcop_->CreateTempNamespace(network::connection_id_t(0), "terrier");
    context_.SetDatabaseName("terrier");
    context_.SetDatabaseOid(oids.first);
    context_.SetTempNamespaceOid(oids.second);
    db_oid_ = oids.first;

    ExecuteSQL("CREATE TABLE foo (col1 INT, col2 INT, col3 INT);", network::QueryType::QUERY_CREATE_TABLE);
    ExecuteSQL("CREATE TABLE bar (col1 INT, col2 INT, col3 INT);", network::QueryType::QUERY_CREATE_TABLE);
    ExecuteSQL("CREATE INDEX bar_idx ON bar (col1, col2);", network::QueryType::QUERY_CREATE_INDEX);

    ExecuteSQL("INSERT INTO foo VALUES (1, 2, 3);", network::QueryType::QUERY_INSERT);
    ExecuteSQL("INSERT INTO foo VALUES (1, 4, 5);", network::QueryType::QUERY_INSERT);
    ExecuteSQL("INSERT INTO bar VALUES (1, 10, 100);", network::QueryType::QUERY_INSERT);
  }

  void TearDown() override { TerrierTest::TearDown(); }

  network::ConnectionContext context_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<trafficcop::TrafficCop> tcop_;
  std::unique_ptr<DBMain> db_main_;
  catalog::db_oid_t db_oid_;
};

// NOLINTNEXTLINE
TEST_F(IdxJoinTest, SimpleIdxJoinTest) {
  auto sql = "SELECT foo.col1, foo.col2, foo.col3, bar.col2 FROM foo, bar WHERE foo.col1 = bar.col1";

  auto txn = txn_manager_->BeginTransaction();
  auto stmt_list = parser::PostgresParser::BuildParseTree(sql);

  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid_);
  auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid_);
  binder.BindNameToNode(common::ManagedPointer(stmt_list), nullptr);

  auto cost_model = std::make_unique<optimizer::IndexFitCostModel>();
  auto out_plan = trafficcop::TrafficCopUtil::Optimize(
      common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid_,
      db_main_->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);

  EXPECT_EQ(out_plan->GetPlanNodeType(), planner::PlanNodeType::INDEXNLJOIN);

  uint32_t num_output_rows{0};
  execution::compiler::RowChecker row_checker = [&num_output_rows](const std::vector<execution::sql::Val *> &vals) {
    num_output_rows++;
  };
  execution::compiler::CorrectnessFn correcteness_fn;
  execution::compiler::GenericChecker checker(row_checker, correcteness_fn);

  // Make Exec Ctx
  execution::compiler::OutputStore store{&checker, out_plan->GetOutputSchema().Get()};
  execution::exec::OutputPrinter printer(out_plan->GetOutputSchema().Get());
  execution::compiler::MultiOutputCallback callback{std::vector<execution::exec::OutputCallback>{store, printer}};
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
          db_oid_, common::ManagedPointer(txn), std::move(callback), out_plan->GetOutputSchema().Get(), common::ManagedPointer(accessor));

  // Run & Check
  auto executable = execution::ExecutableQuery(common::ManagedPointer(out_plan), common::ManagedPointer(exec_ctx));
  executable.Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
  checker.CheckCorrectness();

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

}  // namespace terrier::optimizer
