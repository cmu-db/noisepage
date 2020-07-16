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

class SeqScanTranslatorTest : public SqlBasedTest {
 protected:
  void SetUp() override { SqlBasedTest::SetUp(); }
  static void SetUpTestSuite() { tpl::vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { tpl::vm::LLVMEngine::Shutdown(); }

 private:
  tbb::task_scheduler_init anonymous_;
};

// NOLINTNEXTLINE
TEST_F(SeqScanTranslatorTest, SimpleSeqScanTest) {
  // SELECT col1, col2, col1 * col2, col1 >= 100*col2 FROM test_1 WHERE col1 < 500 AND col2 >= 5;
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
    // Make New Column
    auto col3 = expr_maker.OpMul(col1, col2);
    auto col4 = expr_maker.CompareGe(col1, expr_maker.OpMul(expr_maker.Constant(100), col2));
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    seq_scan_out.AddOutput("col3", col3);
    seq_scan_out.AddOutput("col4", col4);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto comp1 = expr_maker.CompareLt(col1, expr_maker.Constant(500));
    auto comp2 = expr_maker.CompareGe(col2, expr_maker.Constant(5));
    auto predicate = expr_maker.ConjunctionAnd(comp1, comp2);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan =
        builder.SetOutputSchema(std::move(schema)).SetScanPredicate(predicate).SetTableOid(table->GetId()).Build();
  }

  auto last = seq_scan.get();

  // Make the output checkers
  SingleIntComparisonChecker col1_checker(std::less<int64_t>(), 0, 500);
  SingleIntComparisonChecker col2_checker(std::greater_equal<int64_t>(), 1, 5);
  MultiChecker multi_checker({&col1_checker, &col2_checker});

  // Create the execution context
  OutputCollectorAndChecker store(&multi_checker, last->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last->GetOutputSchema(), &callback);
  // Run & Check
  auto query = CompilationContext::Compile(*last);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(SeqScanTranslatorTest, NonVecFilterTest) {
  // SELECT col1, col2, col1 * col2, col1 >= 100*col2 FROM test_1
  // WHERE (col1 < 500 AND col2 >= 5) OR (500 <= col1 <= 1000 AND (col2 = 3 OR col2 = 7));
  // The filter is not in DNF form and can't be vectorized.
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *table = accessor->LookupTableByName("test_1");
  const auto &table_schema = table->GetSchema();
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumnInfo("colA").oid, sql::TypeId::Integer);
    auto col2 = expr_maker.CVE(table_schema.GetColumnInfo("colB").oid, sql::TypeId::Integer);
    // Make New Column
    auto col3 = expr_maker.OpMul(col1, col2);
    auto col4 = expr_maker.CompareGe(col1, expr_maker.OpMul(expr_maker.Constant(100), col2));
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    seq_scan_out.AddOutput("col3", col3);
    seq_scan_out.AddOutput("col4", col4);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto comp1 = expr_maker.CompareLt(col1, expr_maker.Constant(500));
    auto comp2 = expr_maker.CompareGe(col2, expr_maker.Constant(5));
    auto clause1 = expr_maker.ConjunctionAnd(comp1, comp2);
    auto comp3 = expr_maker.CompareGe(col1, expr_maker.Constant(500));
    auto comp4 = expr_maker.CompareLt(col1, expr_maker.Constant(1000));
    auto comp5 = expr_maker.CompareEq(col2, expr_maker.Constant(3));
    auto comp6 = expr_maker.CompareEq(col2, expr_maker.Constant(7));
    auto clause2 =
        expr_maker.ConjunctionAnd(expr_maker.ConjunctionAnd(comp3, comp4), expr_maker.ConjunctionOr(comp5, comp6));
    auto predicate = expr_maker.ConjunctionOr(clause1, clause2);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan =
        builder.SetOutputSchema(std::move(schema)).SetScanPredicate(predicate).SetTableOid(table->GetId()).Build();
  }
  auto last = seq_scan.get();
  // Make the output checkers
  // Make the checkers
  uint32_t num_output_rows = 0;
  RowChecker row_checker = [&num_output_rows](const std::vector<const sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<const sql::Integer *>(vals[0]);
    auto col2 = static_cast<const sql::Integer *>(vals[1]);
    ASSERT_FALSE(col1->is_null || col2->is_null);
    // Check predicate
    ASSERT_TRUE((col1->val < 500 && col2->val >= 5) ||
                (col1->val >= 500 && col1->val < 1000 && (col2->val == 7 || col2->val == 3)));
    num_output_rows++;
  };
  GenericChecker checker(row_checker, {});

  // Create the execution context
  OutputCollectorAndChecker store(&checker, last->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, last->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*last);
  query->Run(&exec_ctx);

  checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(SeqScanTranslatorTest, SeqScanWithProjection) {
  // SELECT col1, col2, col1 * col1, col1 >= 100*col2 FROM test_1 WHERE col1 < 500;
  // This test first selects col1 and col2, and then constructs the 4 output columns.

  auto accessor = sql::Catalog::Instance();
  sql::Table *table = accessor->LookupTableByName("test_1");
  const auto &table_schema = table->GetSchema();

  planner::ExpressionMaker expr_maker;
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumnInfo("colA").oid, sql::TypeId::Integer);
    auto col2 = expr_maker.CVE(table_schema.GetColumnInfo("colB").oid, sql::TypeId::Integer);
    // Make New Column
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.CompareLt(col1, expr_maker.Constant(500));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan =
        builder.SetOutputSchema(std::move(schema)).SetScanPredicate(predicate).SetTableOid(table->GetId()).Build();
  }

  std::unique_ptr<planner::AbstractPlanNode> proj;
  planner::OutputSchemaHelper proj_out(&expr_maker, 0);
  {
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    auto col3 = expr_maker.OpMul(col1, col1);
    auto col4 = expr_maker.CompareGe(col1, expr_maker.OpMul(expr_maker.Constant(100), col2));
    proj_out.AddOutput("col1", col1);
    proj_out.AddOutput("col2", col2);
    proj_out.AddOutput("col3", col3);
    proj_out.AddOutput("col4", col4);
    auto schema = proj_out.MakeSchema();
    planner::ProjectionPlanNode::Builder builder;
    proj = builder.SetOutputSchema(std::move(schema)).AddChild(std::move(seq_scan)).Build();
  }

  // Make the output checkers:
  // 1. There has to be exactly 500 rows, due to the filter.
  // 2. col1 must be less than 500, due to the filter.
  TupleCounterChecker num_checker(500);
  SingleIntComparisonChecker col1_checker(std::less<>(), 0, 500);
  GenericChecker col1_issquare_checker(
      [](const std::vector<const sql::Val *> &vals) {
        // Ensure col3, which should be col1*col1, is indeed a perfect square.
        auto col3 = static_cast<const sql::Integer *>(vals[2]);
        EXPECT_FALSE(col3->is_null);
        const auto root = std::round(std::sqrt(col3->val));
        EXPECT_EQ(col3->val, root * root);
      },
      nullptr);
  MultiChecker multi_checker({&num_checker, &col1_checker, &col1_issquare_checker});

  // Create the execution context
  OutputCollectorAndChecker store{&multi_checker, proj->GetOutputSchema()};
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, proj->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*proj);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

}  // namespace tpl::sql::codegen
#endif
