#include <memory>

#include "sql/catalog.h"
#include "sql/codegen/compilation_context.h"
#include "sql/execution_context.h"
#include "sql/planner/plannodes/limit_plan_node.h"
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

class LimitTranslatorTest : public SqlBasedTest {
 protected:
  void SetUp() override { SqlBasedTest::SetUp(); }
  static void SetUpTestSuite() { tpl::vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { tpl::vm::LLVMEngine::Shutdown(); }

 private:
  tbb::task_scheduler_init anonymous_;
};

namespace {

void TestLimitAndOrOffset(uint64_t off, uint64_t lim) {
  // SELECT col1, col2 FROM small_1 OFFSET off LIMIT lim;
  // small_1.col2 is serial, we use that to check offset/limit.

  auto accessor = sql::Catalog::Instance();
  planner::ExpressionMaker expr_maker;
  sql::Table *table = accessor->LookupTableByName("small_1");
  const auto &table_schema = table->GetSchema();

  // Scan.
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    auto col1 = expr_maker.CVE(table_schema.GetColumnInfo("col1").oid, sql::TypeId::Integer);
    auto col2 = expr_maker.CVE(table_schema.GetColumnInfo("col2").oid, sql::TypeId::Integer);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema)).SetScanPredicate(nullptr).SetTableOid(table->GetId()).Build();
  }

  // Limit.
  std::unique_ptr<planner::AbstractPlanNode> limit;
  planner::OutputSchemaHelper limit_out(&expr_maker, 0);
  {
    // Read previous output
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    // Make the output expressions
    limit_out.AddOutput("col1", col1);
    limit_out.AddOutput("col2", col2);
    auto schema = limit_out.MakeSchema();
    // Build
    limit = planner::LimitPlanNode::Builder()
                .SetLimit(lim)
                .SetOffset(off)
                .SetOutputSchema(std::move(schema))
                .AddChild(std::move(seq_scan))
                .Build();
  }

  // Make the checkers
  GenericChecker row_check(
      [&](const std::vector<const sql::Val *> &row) {
        // col2 is monotonically increasing from 0. So, the values we get back
        // should match: off <= col2 < off+lim.
        const auto col2 = static_cast<const sql::Integer *>(row[1]);
        EXPECT_GE(col2->val, off);
        EXPECT_LT(col2->val, off + lim);
      },
      nullptr);
  uint32_t expected_tuple_count = 0;
  if (lim == 0) {
    expected_tuple_count = table->GetTupleCount() > off ? table->GetTupleCount() - off : 0;
  } else {
    expected_tuple_count = table->GetTupleCount() > off ? std::min(lim, table->GetTupleCount() - off) : 0;
  }
  TupleCounterChecker num_checker(expected_tuple_count);
  MultiChecker multi_checker({&num_checker, &row_check});

  // Compile and Run
  OutputCollectorAndChecker store(&multi_checker, limit->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, limit->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*limit);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

}  // namespace

TEST_F(LimitTranslatorTest, LimitAndOffsetTest) {
  // We don't test zero-limits because those should've been optimized out before
  // ever getting to query execution.
  TestLimitAndOrOffset(0, 1);
  TestLimitAndOrOffset(0, 10);
  TestLimitAndOrOffset(10, 20);
  TestLimitAndOrOffset(10, 1);
  TestLimitAndOrOffset(50, 20);
  TestLimitAndOrOffset(10000, 0);
  TestLimitAndOrOffset(10000, 10000);
}

}  // namespace tpl::sql::codegen
