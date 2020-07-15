#if 0
#include <memory>

#include "sql/catalog.h"
#include "sql/codegen/compilation_context.h"
#include "sql/execution_context.h"
#include "sql/planner/plannodes/nested_loop_join_plan_node.h"
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

class NestedLoopJoinTranslatorTest : public SqlBasedTest {
 protected:
  void SetUp() override { SqlBasedTest::SetUp(); }
  static void SetUpTestSuite() { tpl::vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { tpl::vm::LLVMEngine::Shutdown(); }

 private:
  tbb::task_scheduler_init anonymous_;
};

// NOLINTNEXTLINE
TEST_F(NestedLoopJoinTranslatorTest, SimpleNestedLoopJoinTest) {
  // Self join:
  // SELECT t1.col1, t1.col2, t2.col2, t1.col1 + t2.col2
  //   FROM small_1 AS t1 INNER JOIN small_1 AS t2 ON t1.col2 = t2.col2
  //  WHERE t2.col2 < 80;

  // Get accessor
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;

  // Scan small_1.
  std::unique_ptr<planner::AbstractPlanNode> seq_scan1;
  planner::OutputSchemaHelper seq_scan_out1(&expr_maker, 0);
  {
    auto table1 = accessor->LookupTableByName("small_1");
    auto &table_schema1 = table1->GetSchema();
    // Add col1 and col2 to output.
    auto col1 = expr_maker.CVE(table_schema1.GetColumnInfo("col1").oid, TypeId::Integer);
    auto col2 = expr_maker.CVE(table_schema1.GetColumnInfo("col2").oid, TypeId::Integer);
    seq_scan_out1.AddOutput("col1", col1);
    seq_scan_out1.AddOutput("col2", col2);
    auto schema = seq_scan_out1.MakeSchema();
    // Build.
    planner::SeqScanPlanNode::Builder builder;
    seq_scan1 = builder.SetOutputSchema(std::move(schema)).SetTableOid(table1->GetId()).Build();
  }

  // Scan small_1.
  std::unique_ptr<planner::AbstractPlanNode> seq_scan2;
  planner::OutputSchemaHelper seq_scan_out2(&expr_maker, 1);
  {
    auto table2 = accessor->LookupTableByName("small_1");
    auto &table_schema2 = table2->GetSchema();
    // Add col1 and col2 to output.
    auto col1 = expr_maker.CVE(table_schema2.GetColumnInfo("col1").oid, TypeId::Integer);
    auto col2 = expr_maker.CVE(table_schema2.GetColumnInfo("col2").oid, TypeId::Integer);
    seq_scan_out2.AddOutput("col1", col1);
    seq_scan_out2.AddOutput("col2", col2);
    auto schema = seq_scan_out2.MakeSchema();
    // Make predicate: col2 < 80.
    auto predicate = expr_maker.CompareLt(col2, expr_maker.Constant(80));
    // Build.
    planner::SeqScanPlanNode::Builder builder;
    seq_scan2 =
        builder.SetOutputSchema(std::move(schema)).SetScanPredicate(predicate).SetTableOid(table2->GetId()).Build();
  }

  // NLJ plan.
  std::unique_ptr<planner::AbstractPlanNode> nl_join;
  planner::OutputSchemaHelper nl_join_out(&expr_maker, 0);
  {
    // t1.col1 and t1.col2
    auto t1_col1 = seq_scan_out1.GetOutput("col1");
    auto t1_col2 = seq_scan_out1.GetOutput("col2");
    // t1.col1 and t2.col2
    auto t2_col2 = seq_scan_out2.GetOutput("col2");
    // t1.col1 + t2.col2
    auto sum = expr_maker.OpSum(t1_col1, t2_col2);
    // Output Schema.
    nl_join_out.AddOutput("t1.col1", t1_col1);
    nl_join_out.AddOutput("t1.col2", t1_col2);
    nl_join_out.AddOutput("t2.col2", t2_col2);
    nl_join_out.AddOutput("sum", sum);
    auto schema = nl_join_out.MakeSchema();
    // Predicate.
    auto predicate = expr_maker.CompareEq(t1_col2, t2_col2);
    // Build.
    planner::NestedLoopJoinPlanNode::Builder builder;
    nl_join = builder.AddChild(std::move(seq_scan1))
                  .AddChild(std::move(seq_scan2))
                  .SetOutputSchema(std::move(schema))
                  .SetJoinType(planner::LogicalJoinType::INNER)
                  .SetJoinPredicate(predicate)
                  .Build();
  }

  // Checkers:
  // 1. Only 80 rows should be produced due to where clause.
  // 2. Joined columns should be equal.
  // 3. The 4th column is the sum of the 1rst and 3rd columns.
  TupleCounterChecker tuple_count_check(80);
  SingleIntJoinChecker join_col_check(1, 2);
  GenericChecker row_check(
      [](const std::vector<const sql::Val *> &vals) {
        // Check that col4 = col1 + col3.
        auto col1 = static_cast<const sql::Integer *>(vals[0]);
        auto col3 = static_cast<const sql::Integer *>(vals[2]);
        auto col4 = static_cast<const sql::Integer *>(vals[3]);
        ASSERT_EQ(col4->val, col1->val + col3->val);
      },
      nullptr);
  MultiChecker multi_check({&tuple_count_check, &join_col_check, &row_check});

  // Setup for execution.
  OutputCollectorAndChecker store(&multi_check, nl_join->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, nl_join->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*nl_join);
  query->Run(&exec_ctx);

  multi_check.CheckCorrectness();
}

}  // namespace tpl::sql::codegen
#endif