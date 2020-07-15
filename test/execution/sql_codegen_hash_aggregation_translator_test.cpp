#if 0
#include <memory>

#include "sql/catalog.h"
#include "sql/codegen/compilation_context.h"
#include "sql/execution_context.h"
#include "sql/planner/plannodes/aggregate_plan_node.h"
#include "sql/planner/plannodes/projection_plan_node.h"
#include "sql/planner/plannodes/seq_scan_plan_node.h"
#include "sql/printing_consumer.h"
#include "sql/schema.h"
#include "sql/table.h"
#include "tbb/tbb.h"
#include "util/sql_test_harness.h"
#include "vm/llvm_engine.h"

// Tests
#include "sql/codegen/output_checker.h"
#include "sql/planner/expression_maker.h"
#include "sql/planner/output_schema_util.h"

namespace tpl::sql::codegen {

using namespace std::chrono_literals;  // NOLINT

class HashAggregationTranslatorTest : public SqlBasedTest {
 protected:
  void SetUp() override { SqlBasedTest::SetUp(); }
  static void SetUpTestSuite() { tpl::vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { tpl::vm::LLVMEngine::Shutdown(); }

 private:
  tbb::task_scheduler_init anonymous_;
};

// NOLINTNEXTLINE
TEST_F(HashAggregationTranslatorTest, SimpleAggregateTest) {
  // SELECT col2, SUM(col1) FROM test_1 WHERE col1 < 1000 GROUP BY col2;
  // Get accessor
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *table = accessor->LookupTableByName("test_1");
  const auto &table_schema = table->GetSchema();
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out{&expr_maker, 0};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumnInfo("colA").oid, sql::TypeId::Integer);
    auto col2 = expr_maker.CVE(table_schema.GetColumnInfo("colB").oid, sql::TypeId::Integer);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.CompareLt(col1, expr_maker.Constant(1000));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan =
        builder.SetOutputSchema(std::move(schema)).SetScanPredicate(predicate).SetTableOid(table->GetId()).Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  planner::OutputSchemaHelper agg_out{&expr_maker, 0};
  {
    // Read previous output
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    // Add group by term
    agg_out.AddGroupByTerm("col2", col2);
    // Add aggregates
    agg_out.AddAggTerm("sum_col1", expr_maker.AggSum(col1));
    // Make the output expressions
    agg_out.AddOutput("col2", agg_out.GetGroupByTermForOutput("col2"));
    agg_out.AddOutput("sum_col1", agg_out.GetAggTermForOutput("sum_col1"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(agg_out.GetGroupByTerm("col2"))
              .AddAggregateTerm(agg_out.GetAggTerm("col1"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  auto last = agg.get();
  // Make the checkers
  TupleCounterChecker num_checker(10);
  SingleIntSumChecker sum_checker(1, (1000 * 999) / 2);
  MultiChecker multi_checker({&num_checker, &sum_checker});

  // Compile and Run
  OutputCollectorAndChecker store(&multi_checker, agg->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, agg->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*last);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

}  // namespace tpl::sql::codegen
#endif