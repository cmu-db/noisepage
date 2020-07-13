#include "util/sql_test_harness.h"

#include <memory>

#include "tbb/tbb.h"

#include "sql/codegen/compilation_context.h"
#include "sql/execution_context.h"
#include "sql/planner/plannodes/csv_scan_plan_node.h"
#include "sql/printing_consumer.h"
#include "sql/schema.h"

#include "vm/llvm_engine.h"

// Tests
#include "sql/codegen/output_checker.h"
#include "sql/planner/expression_maker.h"
#include "sql/planner/output_schema_util.h"

namespace tpl::sql::codegen {

using namespace std::chrono_literals;  // NOLINT

class CSVScanTranslatorTest : public SqlBasedTest {
 protected:
  void SetUp() override { SqlBasedTest::SetUp(); }
  static void SetUpTestSuite() { tpl::vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { tpl::vm::LLVMEngine::Shutdown(); }

 private:
  tbb::task_scheduler_init anonymous_;
};

TEST_F(CSVScanTranslatorTest, ManyTypesTest) {
  planner::ExpressionMaker expr_maker;

  std::unique_ptr<planner::AbstractPlanNode> csv_scan;
  planner::OutputSchemaHelper seq_scan_out(&expr_maker, 0);
  {
    auto col1 = expr_maker.CVE(0, sql::TypeId::TinyInt);
    auto col2 = expr_maker.CVE(1, sql::TypeId::SmallInt);
    auto col3 = expr_maker.CVE(2, sql::TypeId::Integer);
    auto col4 = expr_maker.CVE(3, sql::TypeId::BigInt);
    auto col5 = expr_maker.CVE(4, sql::TypeId::Float);
    auto col6 = expr_maker.CVE(5, sql::TypeId::Varchar);

    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    seq_scan_out.AddOutput("col3", col3);
    seq_scan_out.AddOutput("col4", col4);
    seq_scan_out.AddOutput("col5", col5);
    seq_scan_out.AddOutput("col6", col6);

    auto schema = seq_scan_out.MakeSchema();
    // Build
    csv_scan = planner::CSVScanPlanNode::Builder()
                   .SetOutputSchema(std::move(schema))
                   .SetFileName("/home/pmenon/work/tpl/test.csv")
                   .SetScanPredicate(nullptr)
                   .Build();
  }

  // Make the checkers:
  // 1. There should be only one output.
  // 2. The count should be size of the table.
  // 3. The sum should be sum from [1,N] where N=num_tuples.
  MultiChecker multi_checker({});

  // Compile and Run
  OutputCollectorAndChecker store(&multi_checker, csv_scan->GetOutputSchema());
  PrintingConsumer consumer(std::cout, csv_scan->GetOutputSchema());
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, csv_scan->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*csv_scan);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

}  // namespace tpl::sql::codegen
