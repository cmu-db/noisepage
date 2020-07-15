#if 0
#include "util/sql_test_harness.h"

#include <memory>

#include "tbb/tbb.h"

#include "sql/catalog.h"
#include "sql/codegen/compilation_context.h"
#include "sql/execution_context.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"
#include "sql/planner/plannodes/projection_plan_node.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/printing_consumer.h"
#include "sql/schema.h"
#include "sql/table.h"

#include "vm/llvm_engine.h"

// Tests
#include "sql/codegen/output_checker.h"
#include "sql/planner/expression_maker.h"
#include "sql/planner/output_schema_util.h"

namespace tpl::sql::codegen {

using namespace std::chrono_literals;  // NOLINT

class StaticAggregationTranslatorTest : public SqlBasedTest {
 protected:
  void SetUp() override { SqlBasedTest::SetUp(); }
  static void SetUpTestSuite() { tpl::vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { tpl::vm::LLVMEngine::Shutdown(); }

 private:
  tbb::task_scheduler_init anonymous_;
};

// NOLINTNEXTLINE
TEST_F(StaticAggregationTranslatorTest, SimpleTest) {
  // SELECT COUNT(*), SUM(cola) FROM test_1;

  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  auto table = accessor->LookupTableByName("test_1");
  auto &table_schema = table->GetSchema();
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    auto col1 = expr_maker.CVE(table_schema.GetColumnInfo("colA").oid, sql::TypeId::Integer);
    seq_scan_out.AddOutput("col1", col1);
    auto schema = seq_scan_out.MakeSchema();
    // Build
    seq_scan = planner::SeqScanPlanNode::Builder()
                   .SetOutputSchema(std::move(schema))
                   .SetScanPredicate(nullptr)
                   .SetTableOid(table->GetId())
                   .Build();
  }

  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out(&expr_maker, 0);
  {
    // Read previous output
    auto col1 = seq_scan_out.GetOutput("col1");
    // Add aggregates
    agg_out.AddAggTerm("count_star", expr_maker.AggCountStar());
    agg_out.AddAggTerm("sum_col1", expr_maker.AggSum(col1));
    // Make the output expressions
    agg_out.AddOutput("count_star", agg_out.GetAggTermForOutput("count_star"));
    agg_out.AddOutput("sum_col1", agg_out.GetAggTermForOutput("sum_col1"));
    auto schema = agg_out.MakeSchema();
    // Build
    agg = planner::AggregatePlanNode::Builder()
              .SetOutputSchema(std::move(schema))
              .AddAggregateTerm(agg_out.GetAggTerm("count_star"))
              .AddAggregateTerm(agg_out.GetAggTerm("sum_col1"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }

  // Make the checkers:
  // 1. There should be only one output.
  // 2. The count should be size of the table.
  // 3. The sum should be sum from [1,N] where N=num_tuples.
  const uint64_t num_tuples = table->GetTupleCount();
  TupleCounterChecker num_checker(1);
  SingleIntComparisonChecker count_checker(std::equal_to<>(), 0, num_tuples);
  SingleIntSumChecker sum_checker(1, num_tuples * (num_tuples - 1) / 2);
  MultiChecker multi_checker({&num_checker, &count_checker, &sum_checker});

  // Compile and Run
  OutputCollectorAndChecker store(&multi_checker, agg->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, agg->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*agg);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(StaticAggregationTranslatorTest, StaticAggregateWithHavingTest) {
  // SELECT COUNT(*) FROM test_1 HAVING COUNT(*) < 0;

  // Make the sequential scan.
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  auto table = accessor->LookupTableByName("test_1");
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema)).SetScanPredicate(nullptr).SetTableOid(table->GetId()).Build();
  }

  // Make the aggregate.
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out(&expr_maker, 0);
  {
    // Add aggregates.
    agg_out.AddAggTerm("count_star", expr_maker.AggCountStar());
    // Make the output expressions.
    agg_out.AddOutput("count_star", agg_out.GetAggTermForOutput("count_star"));
    // Having predicate.
    auto having = expr_maker.CompareLt(agg_out.GetAggTermForOutput("count_star"), expr_maker.Constant(0));
    // Build
    auto schema = agg_out.MakeSchema();
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddAggregateTerm(agg_out.GetAggTerm("count_star"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::PLAIN)
              .SetHavingClausePredicate(having)
              .Build();
  }

  // Make the checkers:
  // 1. Should not output anything since the count is greater than 0.
  TupleCounterChecker num_checker(0);
  MultiChecker multi_checker({&num_checker});

  // Compile and Run
  OutputCollectorAndChecker store(&multi_checker, agg->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, agg->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*agg);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

}  // namespace tpl::sql::codegen
#endif