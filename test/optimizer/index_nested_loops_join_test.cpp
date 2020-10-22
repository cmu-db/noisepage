#include <memory>
#include <stack>
#include <utility>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/compiler/output_checker.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/sql/value.h"
#include "execution/vm/module.h"
#include "main/db_main.h"
#include "network/connection_context.h"
#include "network/network_io_utils.h"
#include "network/postgres/portal.h"
#include "network/postgres/postgres_packet_writer.h"
#include "network/postgres/statement.h"
#include "optimizer/binding.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/optimizer_task.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/pattern.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "test_util/test_harness.h"
#include "traffic_cop/traffic_cop_defs.h"

namespace terrier::optimizer {

struct IdxJoinTest : public TerrierTest {
  const uint64_t optimizer_timeout_ = 1000000;

  void CompileAndRun(std::unique_ptr<planner::AbstractPlanNode> *plan, network::Statement *stmt) {
    network::WriteQueue queue;
    auto pwriter = network::PostgresPacketWriter(common::ManagedPointer(&queue));
    auto portal = network::Portal(common::ManagedPointer(stmt));
    stmt->SetPhysicalPlan(std::move(*plan));
    auto result = tcop_->CodegenPhysicalPlan(common::ManagedPointer(&context_), common::ManagedPointer(&pwriter),
                                             common::ManagedPointer(&portal));
    TERRIER_ASSERT(result.type_ == trafficcop::ResultType::COMPLETE, "Codegen should have succeeded");
    result = tcop_->RunExecutableQuery(common::ManagedPointer(&context_), common::ManagedPointer(&pwriter),
                                       common::ManagedPointer(&portal));
    TERRIER_ASSERT(result.type_ == trafficcop::ResultType::COMPLETE, "Execute should have succeeded");
  }

  void ExecuteCreate(std::unique_ptr<planner::AbstractPlanNode> *plan, network::QueryType qtype) {
    auto result =
        tcop_->ExecuteCreateStatement(common::ManagedPointer(&context_), common::ManagedPointer(*plan), qtype);
    TERRIER_ASSERT(result.type_ == trafficcop::ResultType::COMPLETE, "Execute should have succeeded");
  }

  void ExecuteSQL(std::string sql, network::QueryType qtype) {
    std::vector<parser::ConstantValueExpression> params;
    tcop_->BeginTransaction(common::ManagedPointer(&context_));
    auto parse = tcop_->ParseQuery(sql, common::ManagedPointer(&context_));
    auto stmt = network::Statement(std::move(sql), std::move(std::get<std::unique_ptr<parser::ParseResult>>(parse)));
    auto result = tcop_->BindQuery(common::ManagedPointer(&context_), common::ManagedPointer(&stmt),
                                   common::ManagedPointer(&params));
    TERRIER_ASSERT(result.type_ == trafficcop::ResultType::COMPLETE, "Bind should have succeeded");

    auto plan = tcop_->OptimizeBoundQuery(common::ManagedPointer(&context_), stmt.ParseResult());
    if (qtype >= network::QueryType::QUERY_CREATE_TABLE && qtype != network::QueryType::QUERY_CREATE_INDEX) {
      ExecuteCreate(&plan, qtype);
    } else if (qtype == network::QueryType::QUERY_CREATE_INDEX) {
      ExecuteCreate(&plan, qtype);
      CompileAndRun(&plan, &stmt);
    } else {
      CompileAndRun(&plan, &stmt);
    }

    tcop_->EndTransaction(common::ManagedPointer(&context_), network::QueryType::QUERY_COMMIT);
  }

  void SetUp() override {
    TerrierTest::SetUp();

    db_main_ = terrier::DBMain::Builder()
                   .SetUseGC(true)
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
    ExecuteSQL("CREATE INDEX foo_idx ON foo (col2);", network::QueryType::QUERY_CREATE_INDEX);
    ExecuteSQL("CREATE INDEX bar_idx ON bar (col1, col2, col3);", network::QueryType::QUERY_CREATE_INDEX);

    for (int i = 1; i <= 3; i++)
      for (int j = 11; j <= 13; j++)
        for (int z = 31; z <= 33; z++) {
          std::stringstream query;
          query << "INSERT INTO foo VALUES (";
          query << i << "," << j << "," << z << ")";
          ExecuteSQL(query.str(), network::QueryType::QUERY_INSERT);
        }

    for (int i = 1; i <= 3; i++)
      for (int j = 11; j <= 13; j++)
        for (int z = 301; z <= 303; z++) {
          std::stringstream query;
          query << "INSERT INTO bar VALUES (";
          query << i << "," << j << "," << z << ")";
          ExecuteSQL(query.str(), network::QueryType::QUERY_INSERT);
        }
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
  auto sql =
      "SELECT foo.col1, foo.col2, foo.col3, bar.col1, bar.col2, bar.col3 "
      "FROM foo, bar WHERE foo.col1 = bar.col1 "
      "ORDER BY foo.col1, bar.col1, foo.col2, bar.col2, foo.col3, bar.col3";

  auto txn = txn_manager_->BeginTransaction();
  auto stmt_list = parser::PostgresParser::BuildParseTree(sql);

  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid_, DISABLED);
  auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid_);
  binder.BindNameToNode(common::ManagedPointer(stmt_list), nullptr, nullptr);

  auto cost_model = std::make_unique<optimizer::TrivialCostModel>();
  auto out_plan = trafficcop::TrafficCopUtil::Optimize(
      common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid_,
      db_main_->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);

  EXPECT_EQ(out_plan->GetPlanNodeType(), planner::PlanNodeType::PROJECTION);
  EXPECT_EQ(out_plan->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::ORDERBY);
  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXNLJOIN);
  auto idx_join = reinterpret_cast<const planner::IndexJoinPlanNode *>(out_plan->GetChild(0)->GetChild(0));
  EXPECT_EQ(idx_join->GetHiIndexColumns().size(), 1);

  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::SEQSCAN);

  uint32_t num_output_rows{0};
  uint32_t num_expected_rows = 243;
  execution::compiler::test::RowChecker row_checker =
      [&num_output_rows](const std::vector<execution::sql::Val *> &vals) {
        num_output_rows++;

        // Read cols
        auto foo_col1 = static_cast<execution::sql::Integer *>(vals[0]);
        auto foo_col2 = static_cast<execution::sql::Integer *>(vals[1]);
        auto foo_col3 = static_cast<execution::sql::Integer *>(vals[2]);
        auto bar_col1 = static_cast<execution::sql::Integer *>(vals[3]);
        auto bar_col2 = static_cast<execution::sql::Integer *>(vals[4]);
        auto bar_col3 = static_cast<execution::sql::Integer *>(vals[5]);
        ASSERT_FALSE(foo_col1->is_null_ || foo_col2->is_null_ || foo_col3->is_null_ || bar_col1->is_null_ ||
                     bar_col2->is_null_ || bar_col3->is_null_);

        ASSERT_EQ(foo_col1->val_, 1 + ((num_output_rows - 1) / 81));
        ASSERT_EQ(foo_col1->val_, bar_col1->val_);

        ASSERT_GE(foo_col2->val_, 11);
        ASSERT_GE(bar_col2->val_, 11);
        ASSERT_LE(foo_col2->val_, 13);
        ASSERT_LE(bar_col2->val_, 13);

        ASSERT_GE(foo_col3->val_, 31);
        ASSERT_LE(foo_col3->val_, 33);
        ASSERT_GE(bar_col3->val_, 301);
        ASSERT_LE(bar_col3->val_, 303);
      };

  execution::compiler::test::CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  execution::compiler::test::GenericChecker checker(row_checker, correctness_fn);

  // Make Exec Ctx
  execution::compiler::test::OutputStore store{&checker, out_plan->GetOutputSchema().Get()};
  execution::exec::OutputPrinter printer(out_plan->GetOutputSchema().Get());
  execution::compiler::test::MultiOutputCallback callback{std::vector<execution::exec::OutputCallback>{store, printer}};
  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.is_parallel_execution_enabled_ = false;
  execution::exec::OutputCallback callback_fn = callback.ConstructOutputCallback();
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid_, common::ManagedPointer(txn), callback_fn, out_plan->GetOutputSchema().Get(),
      common::ManagedPointer(accessor), exec_settings, db_main_->GetMetricsManager());

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*out_plan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 2);

  // TODO(WAN): Right now build_feature and iterate_feature are indistinguishable.

  bool build_feature = false, seq_feature = false, idx_feature = false;
  auto pipe0_vec = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  for (auto &feature : pipe0_vec) {
    switch (feature.GetExecutionOperatingUnitType()) {
      case brain::ExecutionOperatingUnitType::SORT_ITERATE:
        build_feature = true;
        break;
      default:
        break;
    }
  }

  EXPECT_TRUE(build_feature);

  bool iterate_feature = false;
  auto pipe1_vec = pipeline->GetPipelineFeatures(execution::pipeline_id_t(2));
  for (auto &feature : pipe1_vec) {
    switch (feature.GetExecutionOperatingUnitType()) {
      case brain::ExecutionOperatingUnitType::SORT_BUILD:
        iterate_feature = true;
        break;
      case brain::ExecutionOperatingUnitType::SEQ_SCAN:
        seq_feature = true;
        break;
      case brain::ExecutionOperatingUnitType::IDX_SCAN:
        idx_feature = true;
        break;
      default:
        break;
    }
  }
  EXPECT_TRUE(iterate_feature && seq_feature && idx_feature);

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(IdxJoinTest, MultiPredicateJoin) {
  auto sql =
      "SELECT foo.col1, foo.col2, foo.col3, bar.col1, bar.col2, bar.col3 "
      "FROM foo, bar WHERE foo.col1 = bar.col1 and foo.col2 = bar.col2 "
      "ORDER BY foo.col1, bar.col1, foo.col2, bar.col2, foo.col3, bar.col3";

  auto txn = txn_manager_->BeginTransaction();
  auto stmt_list = parser::PostgresParser::BuildParseTree(sql);

  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid_, DISABLED);
  auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid_);
  binder.BindNameToNode(common::ManagedPointer(stmt_list), nullptr, nullptr);

  auto cost_model = std::make_unique<optimizer::TrivialCostModel>();
  auto out_plan = trafficcop::TrafficCopUtil::Optimize(
      common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid_,
      db_main_->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);

  EXPECT_EQ(out_plan->GetPlanNodeType(), planner::PlanNodeType::PROJECTION);
  EXPECT_EQ(out_plan->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::ORDERBY);
  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXNLJOIN);
  auto idx_join = reinterpret_cast<const planner::IndexJoinPlanNode *>(out_plan->GetChild(0)->GetChild(0));
  EXPECT_EQ(idx_join->GetHiIndexColumns().size(), 2);

  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::SEQSCAN);

  uint32_t num_output_rows{0};
  uint32_t num_expected_rows = 81;
  execution::compiler::test::RowChecker row_checker =
      [&num_output_rows](const std::vector<execution::sql::Val *> &vals) {
        num_output_rows++;

        // Read cols
        auto foo_col1 = static_cast<execution::sql::Integer *>(vals[0]);
        auto foo_col2 = static_cast<execution::sql::Integer *>(vals[1]);
        auto foo_col3 = static_cast<execution::sql::Integer *>(vals[2]);
        auto bar_col1 = static_cast<execution::sql::Integer *>(vals[3]);
        auto bar_col2 = static_cast<execution::sql::Integer *>(vals[4]);
        auto bar_col3 = static_cast<execution::sql::Integer *>(vals[5]);
        ASSERT_FALSE(foo_col1->is_null_ || foo_col2->is_null_ || foo_col3->is_null_ || bar_col1->is_null_ ||
                     bar_col2->is_null_ || bar_col3->is_null_);

        ASSERT_EQ(foo_col1->val_, 1 + ((num_output_rows - 1) / 27));
        ASSERT_EQ(foo_col1->val_, bar_col1->val_);

        ASSERT_EQ(foo_col2->val_, bar_col2->val_);
        ASSERT_GE(foo_col2->val_, 11);
        ASSERT_GE(bar_col2->val_, 11);
        ASSERT_LE(foo_col2->val_, 13);
        ASSERT_LE(bar_col2->val_, 13);

        ASSERT_GE(foo_col3->val_, 31);
        ASSERT_LE(foo_col3->val_, 33);
        ASSERT_GE(bar_col3->val_, 301);
        ASSERT_LE(bar_col3->val_, 303);
      };

  execution::compiler::test::CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  execution::compiler::test::GenericChecker checker(row_checker, correctness_fn);

  // Make Exec Ctx
  execution::compiler::test::OutputStore store{&checker, out_plan->GetOutputSchema().Get()};
  execution::exec::OutputPrinter printer(out_plan->GetOutputSchema().Get());
  execution::compiler::test::MultiOutputCallback callback{std::vector<execution::exec::OutputCallback>{store, printer}};
  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.is_parallel_execution_enabled_ = false;
  execution::exec::OutputCallback callback_fn = callback.ConstructOutputCallback();
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid_, common::ManagedPointer(txn), callback_fn, out_plan->GetOutputSchema().Get(),
      common::ManagedPointer(accessor), exec_settings, db_main_->GetMetricsManager());

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*out_plan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
  checker.CheckCorrectness();

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(IdxJoinTest, MultiPredicateJoinWithExtra) {
  auto sql =
      "SELECT foo.col1, foo.col2, foo.col3, bar.col1, bar.col2, bar.col3 "
      "FROM foo, bar WHERE foo.col1 = bar.col1 and foo.col2 = bar.col2 and bar.col3 = 301 "
      "ORDER BY foo.col1, bar.col1, foo.col2, bar.col2, foo.col3, bar.col3";

  auto txn = txn_manager_->BeginTransaction();
  auto stmt_list = parser::PostgresParser::BuildParseTree(sql);

  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid_, DISABLED);
  auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid_);
  binder.BindNameToNode(common::ManagedPointer(stmt_list), nullptr, nullptr);

  auto cost_model = std::make_unique<optimizer::TrivialCostModel>();
  auto out_plan = trafficcop::TrafficCopUtil::Optimize(
      common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid_,
      db_main_->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);

  EXPECT_EQ(out_plan->GetPlanNodeType(), planner::PlanNodeType::PROJECTION);
  EXPECT_EQ(out_plan->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::ORDERBY);
  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXNLJOIN);
  auto idx_join = reinterpret_cast<const planner::IndexJoinPlanNode *>(out_plan->GetChild(0)->GetChild(0));
  EXPECT_EQ(idx_join->GetHiIndexColumns().size(), 3);

  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::SEQSCAN);

  uint32_t num_output_rows{0};
  uint32_t num_expected_rows = 27;
  execution::compiler::test::RowChecker row_checker =
      [&num_output_rows](const std::vector<execution::sql::Val *> &vals) {
        num_output_rows++;

        // Read cols
        auto foo_col1 = static_cast<execution::sql::Integer *>(vals[0]);
        auto foo_col2 = static_cast<execution::sql::Integer *>(vals[1]);
        auto foo_col3 = static_cast<execution::sql::Integer *>(vals[2]);
        auto bar_col1 = static_cast<execution::sql::Integer *>(vals[3]);
        auto bar_col2 = static_cast<execution::sql::Integer *>(vals[4]);
        auto bar_col3 = static_cast<execution::sql::Integer *>(vals[5]);
        ASSERT_FALSE(foo_col1->is_null_ || foo_col2->is_null_ || foo_col3->is_null_ || bar_col1->is_null_ ||
                     bar_col2->is_null_ || bar_col3->is_null_);

        ASSERT_EQ(foo_col1->val_, 1 + ((num_output_rows - 1) / 9));
        ASSERT_EQ(foo_col1->val_, bar_col1->val_);

        ASSERT_EQ(foo_col2->val_, bar_col2->val_);
        ASSERT_GE(foo_col2->val_, 11);
        ASSERT_GE(bar_col2->val_, 11);
        ASSERT_LE(foo_col2->val_, 13);
        ASSERT_LE(bar_col2->val_, 13);

        ASSERT_GE(foo_col3->val_, 31);
        ASSERT_LE(foo_col3->val_, 33);
        ASSERT_EQ(bar_col3->val_, 301);
      };

  execution::compiler::test::CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  execution::compiler::test::GenericChecker checker(row_checker, correctness_fn);

  // Make Exec Ctx
  execution::compiler::test::OutputStore store{&checker, out_plan->GetOutputSchema().Get()};
  execution::exec::OutputPrinter printer(out_plan->GetOutputSchema().Get());
  execution::compiler::test::MultiOutputCallback callback{std::vector<execution::exec::OutputCallback>{store, printer}};
  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.is_parallel_execution_enabled_ = false;
  execution::exec::OutputCallback callback_fn = callback.ConstructOutputCallback();
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid_, common::ManagedPointer(txn), callback_fn, out_plan->GetOutputSchema().Get(),
      common::ManagedPointer(accessor), exec_settings, db_main_->GetMetricsManager());

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*out_plan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
  checker.CheckCorrectness();

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(IdxJoinTest, FooOnlyScan) {
  auto sql =
      "SELECT foo.col1, foo.col2, foo.col3 "
      "FROM foo, bar WHERE foo.col1 = bar.col1 and foo.col2 = bar.col2 "
      "ORDER BY foo.col1, foo.col2, foo.col3";

  auto txn = txn_manager_->BeginTransaction();
  auto stmt_list = parser::PostgresParser::BuildParseTree(sql);

  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid_, DISABLED);
  auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid_);
  binder.BindNameToNode(common::ManagedPointer(stmt_list), nullptr, nullptr);

  auto cost_model = std::make_unique<optimizer::TrivialCostModel>();
  auto out_plan = trafficcop::TrafficCopUtil::Optimize(
      common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid_,
      db_main_->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);

  EXPECT_EQ(out_plan->GetPlanNodeType(), planner::PlanNodeType::PROJECTION);
  EXPECT_EQ(out_plan->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::ORDERBY);
  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXNLJOIN);
  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::SEQSCAN);

  uint32_t num_output_rows{0};
  uint32_t num_expected_rows = 81;
  execution::compiler::test::RowChecker row_checker =
      [&num_output_rows](const std::vector<execution::sql::Val *> &vals) {
        num_output_rows++;

        // Read cols
        auto foo_col1 = static_cast<execution::sql::Integer *>(vals[0]);
        auto foo_col2 = static_cast<execution::sql::Integer *>(vals[1]);
        auto foo_col3 = static_cast<execution::sql::Integer *>(vals[2]);
        ASSERT_FALSE(foo_col1->is_null_ || foo_col2->is_null_ || foo_col3->is_null_);

        ASSERT_EQ(foo_col1->val_, 1 + ((num_output_rows - 1) / 27));
        ASSERT_GE(foo_col2->val_, 11);
        ASSERT_LE(foo_col2->val_, 13);
        ASSERT_GE(foo_col3->val_, 31);
        ASSERT_LE(foo_col3->val_, 33);
      };

  execution::compiler::test::CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  execution::compiler::test::GenericChecker checker(row_checker, correctness_fn);

  // Make Exec Ctx
  execution::compiler::test::OutputStore store{&checker, out_plan->GetOutputSchema().Get()};
  execution::exec::OutputPrinter printer(out_plan->GetOutputSchema().Get());
  execution::compiler::test::MultiOutputCallback callback{std::vector<execution::exec::OutputCallback>{store, printer}};
  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.is_parallel_execution_enabled_ = false;
  execution::exec::OutputCallback callback_fn = callback.ConstructOutputCallback();
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid_, common::ManagedPointer(txn), callback_fn, out_plan->GetOutputSchema().Get(),
      common::ManagedPointer(accessor), exec_settings, db_main_->GetMetricsManager());

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*out_plan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
  checker.CheckCorrectness();

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(IdxJoinTest, BarOnlyScan) {
  auto sql =
      "SELECT bar.col1, bar.col2, bar.col3 "
      "FROM foo, bar WHERE foo.col1 = bar.col1 and foo.col2 = bar.col2 "
      "ORDER BY bar.col1, bar.col2, bar.col3";

  auto txn = txn_manager_->BeginTransaction();
  auto stmt_list = parser::PostgresParser::BuildParseTree(sql);

  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid_, DISABLED);
  auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid_);
  binder.BindNameToNode(common::ManagedPointer(stmt_list), nullptr, nullptr);

  auto cost_model = std::make_unique<optimizer::TrivialCostModel>();
  auto out_plan = trafficcop::TrafficCopUtil::Optimize(
      common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid_,
      db_main_->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);

  EXPECT_EQ(out_plan->GetPlanNodeType(), planner::PlanNodeType::PROJECTION);
  EXPECT_EQ(out_plan->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::ORDERBY);
  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXNLJOIN);
  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::SEQSCAN);

  uint32_t num_output_rows{0};
  uint32_t num_expected_rows = 81;
  execution::compiler::test::RowChecker row_checker =
      [&num_output_rows](const std::vector<execution::sql::Val *> &vals) {
        num_output_rows++;

        // Read cols
        auto bar_col1 = static_cast<execution::sql::Integer *>(vals[0]);
        auto bar_col2 = static_cast<execution::sql::Integer *>(vals[1]);
        auto bar_col3 = static_cast<execution::sql::Integer *>(vals[2]);
        ASSERT_FALSE(bar_col1->is_null_ || bar_col2->is_null_ || bar_col3->is_null_);

        ASSERT_EQ(bar_col1->val_, 1 + ((num_output_rows - 1) / 27));
        ASSERT_GE(bar_col2->val_, 11);
        ASSERT_LE(bar_col2->val_, 13);
        ASSERT_GE(bar_col3->val_, 301);
        ASSERT_LE(bar_col3->val_, 303);
      };

  execution::compiler::test::CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  execution::compiler::test::GenericChecker checker(row_checker, correctness_fn);

  // Make Exec Ctx
  execution::compiler::test::OutputStore store{&checker, out_plan->GetOutputSchema().Get()};
  execution::exec::OutputPrinter printer(out_plan->GetOutputSchema().Get());
  execution::compiler::test::MultiOutputCallback callback{std::vector<execution::exec::OutputCallback>{store, printer}};
  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.is_parallel_execution_enabled_ = false;
  execution::exec::OutputCallback callback_fn = callback.ConstructOutputCallback();
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid_, common::ManagedPointer(txn), callback_fn, out_plan->GetOutputSchema().Get(),
      common::ManagedPointer(accessor), exec_settings, db_main_->GetMetricsManager());

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*out_plan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
  checker.CheckCorrectness();

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(IdxJoinTest, IndexToIndexJoin) {
  auto sql =
      "SELECT foo.col1, foo.col2, foo.col3, bar.col1, bar.col2, bar.col3 "
      "FROM foo, bar WHERE foo.col1 = bar.col1 and foo.col2 = bar.col2 and foo.col2 = 12 and bar.col3 = 302 "
      "ORDER BY foo.col1, bar.col1, foo.col2, bar.col2, foo.col3, bar.col3";

  auto txn = txn_manager_->BeginTransaction();
  auto stmt_list = parser::PostgresParser::BuildParseTree(sql);

  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid_, DISABLED);
  auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid_);
  binder.BindNameToNode(common::ManagedPointer(stmt_list), nullptr, nullptr);

  auto cost_model = std::make_unique<optimizer::TrivialCostModel>();
  auto out_plan = trafficcop::TrafficCopUtil::Optimize(
      common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid_,
      db_main_->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);

  EXPECT_EQ(out_plan->GetPlanNodeType(), planner::PlanNodeType::PROJECTION);
  EXPECT_EQ(out_plan->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::ORDERBY);
  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXNLJOIN);
  auto idx_join = reinterpret_cast<const planner::IndexJoinPlanNode *>(out_plan->GetChild(0)->GetChild(0));
  EXPECT_EQ(idx_join->GetHiIndexColumns().size(), 3);

  EXPECT_EQ(out_plan->GetChild(0)->GetChild(0)->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
  auto idx_scan = reinterpret_cast<const planner::IndexScanPlanNode *>(idx_join->GetChild(0));
  EXPECT_EQ(idx_scan->GetLoIndexColumns().size(), 1);

  uint32_t num_output_rows{0};
  uint32_t num_expected_rows = 9;
  execution::compiler::test::RowChecker row_checker =
      [&num_output_rows](const std::vector<execution::sql::Val *> &vals) {
        num_output_rows++;

        // Read cols
        auto foo_col1 = static_cast<execution::sql::Integer *>(vals[0]);
        auto foo_col2 = static_cast<execution::sql::Integer *>(vals[1]);
        auto foo_col3 = static_cast<execution::sql::Integer *>(vals[2]);
        auto bar_col1 = static_cast<execution::sql::Integer *>(vals[3]);
        auto bar_col2 = static_cast<execution::sql::Integer *>(vals[4]);
        auto bar_col3 = static_cast<execution::sql::Integer *>(vals[5]);
        ASSERT_FALSE(foo_col1->is_null_ || foo_col2->is_null_ || foo_col3->is_null_ || bar_col1->is_null_ ||
                     bar_col2->is_null_ || bar_col3->is_null_);

        ASSERT_EQ(foo_col1->val_, 1 + ((num_output_rows - 1) / 3));
        ASSERT_EQ(foo_col1->val_, bar_col1->val_);

        ASSERT_EQ(foo_col2->val_, 12);
        ASSERT_EQ(bar_col2->val_, 12);

        ASSERT_GE(foo_col3->val_, 31);
        ASSERT_LE(foo_col3->val_, 33);
        ASSERT_EQ(bar_col3->val_, 302);
      };

  execution::compiler::test::CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  execution::compiler::test::GenericChecker checker(row_checker, correctness_fn);

  // Make Exec Ctx
  execution::compiler::test::OutputStore store{&checker, out_plan->GetOutputSchema().Get()};
  execution::exec::OutputPrinter printer(out_plan->GetOutputSchema().Get());
  execution::compiler::test::MultiOutputCallback callback{std::vector<execution::exec::OutputCallback>{store, printer}};
  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.is_parallel_execution_enabled_ = false;
  execution::exec::OutputCallback callback_fn = callback.ConstructOutputCallback();
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid_, common::ManagedPointer(txn), callback_fn, out_plan->GetOutputSchema().Get(),
      common::ManagedPointer(accessor), exec_settings, db_main_->GetMetricsManager());

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*out_plan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
  checker.CheckCorrectness();

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

}  // namespace terrier::optimizer
