#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/output_checker.h"
#include "execution/sql_test.h"
#include "gtest/gtest.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "parser/postgresparser.h"
#include "planner/plannodes/analyze_plan_node.h"
#include "traffic_cop/traffic_cop_util.h"

namespace noisepage::test {

class EndToEndTest : public execution::SqlBasedTest {
 private:
  class NoOpOutputChecker : public execution::compiler::test::OutputChecker {
   public:
    NoOpOutputChecker() = default;
    void CheckCorrectness() override {}
    void ProcessBatch(const std::vector<std::vector<execution::sql::Val *>> &output) override {}
  };

 public:
  EndToEndTest() = default;

  void SetUp() override { SqlBasedTest::SetUp(); }

  void RunQuery(const std::string &query) {
    NoOpOutputChecker no_op_output_checker;
    RunQuery(query, &no_op_output_checker);
  }

  void RunQuery(const std::string &query, execution::compiler::test::OutputChecker *output_checker) {
    // Parse
    auto stmt_list = parser::PostgresParser::BuildParseTree(query);

    auto accessor = MakeAccessor();

    // Bind
    auto *binder = new binder::BindNodeVisitor(common::ManagedPointer(accessor), test_db_oid_);
    // TODO(Joe Koshakow) add support for parameters
    binder->BindNameToNode(common::ManagedPointer(stmt_list.get()), nullptr, nullptr);

    // Optimize
    auto cost_model = std::make_unique<optimizer::TrivialCostModel>();
    auto out_plan =
        trafficcop::TrafficCopUtil::Optimize(common::ManagedPointer(test_txn_), common::ManagedPointer(accessor),
                                             common::ManagedPointer(stmt_list), test_db_oid_, stats_storage_,
                                             std::move(cost_model), 1000000, nullptr)
            ->TakePlanNodeOwnership();

    // This is pretty hacky, but since this skips over some of the traffic cop code, I need to manually add this
    // callback
    if (out_plan->GetPlanNodeType() == planner::PlanNodeType::ANALYZE) {
      const auto analyze_plan = static_cast<planner::AnalyzePlanNode *>(out_plan.get());
      auto db_oid = analyze_plan->GetDatabaseOid();
      auto table_oid = analyze_plan->GetTableOid();
      std::vector<catalog::col_oid_t> col_oids = analyze_plan->GetColumnOids();
      test_txn_->RegisterCommitAction([=]() { stats_storage_->MarkStatsStale(db_oid, table_oid, col_oids); });
    }

    // Execute
    execution::compiler::test::OutputStore store{output_checker, out_plan->GetOutputSchema().Get()};
    execution::compiler::test::MultiOutputCallback callback{std::vector<execution::exec::OutputCallback>{store}};
    execution::exec::OutputCallback callback_fn = callback.ConstructOutputCallback();
    auto exec_ctx = MakeExecCtx(&callback_fn, out_plan->GetOutputSchema().Get());

    auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);

    // Check correctness
    output_checker->CheckCorrectness();

    delete binder;
  }
};

}  // namespace noisepage::test
