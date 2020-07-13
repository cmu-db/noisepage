#include "util/sql_test_harness.h"

#include <memory>

#include "tbb/tbb.h"

#include "sql/catalog.h"
#include "sql/codegen/compilation_context.h"
#include "sql/execution_context.h"
#include "sql/planner/plannodes/order_by_plan_node.h"
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

class SortTranslatorTest : public SqlBasedTest {
 protected:
  void SetUp() override { SqlBasedTest::SetUp(); }
  static void SetUpTestSuite() { tpl::vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { tpl::vm::LLVMEngine::Shutdown(); }

 private:
  tbb::task_scheduler_init anonymous_;
};

TEST_F(SortTranslatorTest, SimpleSortTest) {
  // SELECT col1, col2 FROM test_1 WHERE col1 < 500 ORDER BY col2 ASC

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
    auto predicate = expr_maker.CompareLt(col1, expr_maker.Constant(500));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan =
        builder.SetOutputSchema(std::move(schema)).SetScanPredicate(predicate).SetTableOid(table->GetId()).Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out{&expr_maker, 0};
  {
    // Output Colums col1, col2
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    order_by_out.AddOutput("col1", col1);
    order_by_out.AddOutput("col2", col2);
    auto schema = order_by_out.MakeSchema();
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(seq_scan))
                   .AddSortKey(col2, planner::OrderByOrderingType::ASC)
                   .Build();
  }
  auto last = order_by.get();

  // Checkers:
  // There should be 500 output rows, where col1 < 500.
  // The output should be sorted by col2 ASC
  SingleIntComparisonChecker col1_checker([](auto a, auto b) { return a < b; }, 0, 500);
  SingleIntSortChecker col2_sort_checker(1);
  MultiChecker multi_checker({&col2_sort_checker, &col1_checker});

  OutputCollectorAndChecker store(&multi_checker, order_by->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, order_by->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*last);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

TEST_F(SortTranslatorTest, TwoColumnSortTest) {
  // SELECT col1, col2, col1 + col2 FROM test_1 WHERE col1 < 500 ORDER BY col2 ASC, col1 - col2 DESC

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
    auto predicate = expr_maker.CompareLt(col1, expr_maker.Constant(500));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan =
        builder.SetOutputSchema(std::move(schema)).SetScanPredicate(predicate).SetTableOid(table->GetId()).Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out{&expr_maker, 0};
  {
    // Output Colums col1, col2, col1 + col2
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    auto sum = expr_maker.OpSum(col1, col2);
    order_by_out.AddOutput("col1", col1);
    order_by_out.AddOutput("col2", col2);
    order_by_out.AddOutput("sum", sum);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{col2, planner::OrderByOrderingType::ASC};
    auto diff = expr_maker.OpMin(col1, col2);
    planner::SortKey clause2{diff, planner::OrderByOrderingType::DESC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(seq_scan))
                   .AddSortKey(clause1.first, clause1.second)
                   .AddSortKey(clause2.first, clause2.second)
                   .Build();
  }
  auto last = order_by.get();
  // Checkers:
  // There should be 500 output rows, where col1 < 500.
  // The output should be sorted by col2 ASC, then col1 DESC.
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{500};
  int64_t curr_col1{std::numeric_limits<int64_t>::max()};
  int64_t curr_col2{std::numeric_limits<int64_t>::min()};
  RowChecker row_checker = [&num_output_rows, &curr_col1, &curr_col2,
                            num_expected_rows](const std::vector<const sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<const sql::Integer *>(vals[0]);
    auto col2 = static_cast<const sql::Integer *>(vals[1]);
    ASSERT_FALSE(col1->is_null || col2->is_null);
    // Check col1 and number of outputs
    ASSERT_LT(col1->val, 500);
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
    // Check that output is sorted by col2 ASC, then col1 DESC
    ASSERT_LE(curr_col2, col2->val);
    if (curr_col2 == col2->val) {
      ASSERT_GE(curr_col1, col1->val);
    }
    curr_col1 = col1->val;
    curr_col2 = col2->val;
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  GenericChecker checker(row_checker, correcteness_fn);
  OutputCollectorAndChecker store(&checker, order_by->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, order_by->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*last);
  query->Run(&exec_ctx);

  checker.CheckCorrectness();
}

namespace {

void TestSortWithLimitAndOrOffset(uint64_t off, uint64_t lim) {
  // SELECT col1, col2 FROM test_1 ORDER BY col2 ASC OFFSET off LIMIT lim;

  // Get accessor
  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *table = accessor->LookupTableByName("small_1");
  const auto &table_schema = table->GetSchema();

  // Scan
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out{&expr_maker, 0};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumnInfo("col1").oid, sql::TypeId::Integer);
    auto col2 = expr_maker.CVE(table_schema.GetColumnInfo("col2").oid, sql::TypeId::Integer);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema)).SetTableOid(table->GetId()).Build();
  }

  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  planner::OutputSchemaHelper order_by_out(&expr_maker, 0);
  {
    // Output columns col1, col2
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    order_by_out.AddOutput("col1", col1);
    order_by_out.AddOutput("col2", col2);
    auto schema = order_by_out.MakeSchema();
    // Build
    planner::OrderByPlanNode::Builder builder;
    builder.SetOutputSchema(std::move(schema))
        .AddChild(std::move(seq_scan))
        .AddSortKey(col2, planner::OrderByOrderingType::ASC);
    if (off != 0) builder.SetOffset(off);
    if (lim != 0) builder.SetLimit(lim);
    order_by = builder.Build();
  }

  // Checkers:
  // 1. col2 should contain rows in range [offset, offset+lim].
  // 2. col2 should be sorted by col2 ASC.
  // 3. The total number of rows should depend on offset and limit.
  GenericChecker col2_row_check(
      [&](const std::vector<const sql::Val *> &row) {
        const auto col2 = static_cast<const sql::Integer *>(row[1]);
        EXPECT_GE(col2->val, off);
        if (lim != 0) {
          ASSERT_LT(col2->val, off + lim);
        }
      },
      nullptr);
  uint32_t expected_tuple_count = 0;
  if (lim == 0) {
    expected_tuple_count = table->GetTupleCount() > off ? table->GetTupleCount() - off : 0;
  } else {
    expected_tuple_count = table->GetTupleCount() > off ? std::min(lim, table->GetTupleCount() - off) : 0;
  }
  TupleCounterChecker tuple_count_checker(expected_tuple_count);
  SingleIntSortChecker col2_sort_checker(1);
  MultiChecker multi_checker({&col2_row_check, &col2_sort_checker, &tuple_count_checker});

  OutputCollectorAndChecker store(&multi_checker, order_by->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, order_by->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*order_by);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

}  // namespace

TEST_F(SortTranslatorTest, SortWithLimitAndOffsetTest) {
  TestSortWithLimitAndOrOffset(0, 1);
  TestSortWithLimitAndOrOffset(0, 10);
  TestSortWithLimitAndOrOffset(10, 0);
  TestSortWithLimitAndOrOffset(100, 0);
  TestSortWithLimitAndOrOffset(50000000, 0);
  TestSortWithLimitAndOrOffset(50000000, 50000000);
}

}  // namespace tpl::sql::codegen
