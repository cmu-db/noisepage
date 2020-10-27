#if 0
#include <tbb/tbb.h>

#include <memory>

#include "catalog/schema.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/expression_maker.h"
#include "execution/compiler/output_checker.h"
#include "execution/compiler/output_schema_util.h"
#include "execution/exec/execution_context.h"
#include "execution/sql_test.h"
#include "execution/vm/llvm_engine.h"
#include "planner/plannodes/csv_scan_plan_node.h"

namespace noisepage::execution::sql::codegen::test {

using namespace std::chrono_literals;  // NOLINT

class CSVScanTranslatorTest : public SqlBasedTest {
 protected:
  void SetUp() override { SqlBasedTest::SetUp(); }
  static void SetUpTestSuite() { noisepage::execution::vm::LLVMEngine::Initialize(); }
  static void TearDownTestSuite() { noisepage::execution::vm::LLVMEngine::Shutdown(); }

 private:
  tbb::task_scheduler_init anonymous_;
};

// NOLINTNEXTLINE
TEST_F(CSVScanTranslatorTest, ManyTypesTest) {
  compiler::test::ExpressionMaker expr_maker;

  std::unique_ptr<planner::AbstractPlanNode> csv_scan;
  compiler::test::OutputSchemaHelper seq_scan_out(0, &expr_maker);
  {
    auto col1 = expr_maker.CVE(catalog::col_oid_t(0), type::TypeId::TINYINT);
    auto col2 = expr_maker.CVE(catalog::col_oid_t(1), type::TypeId::SMALLINT);
    auto col3 = expr_maker.CVE(catalog::col_oid_t(2), type::TypeId::INTEGER);
    auto col4 = expr_maker.CVE(catalog::col_oid_t(3), type::TypeId::BIGINT);
    auto col5 = expr_maker.CVE(catalog::col_oid_t(4), type::TypeId::DECIMAL);
    auto col6 = expr_maker.CVE(catalog::col_oid_t(5), type::TypeId::VARCHAR);

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
  compiler::test::MultiChecker multi_checker({});

  // Compile and Run
  OutputCollectorAndChecker store(&multi_checker, csv_scan->GetOutputSchema());
  PrintingConsumer consumer(std::cout, csv_scan->GetOutputSchema());  // NOLINT
  MultiOutputCallback callback({&store});
  sql::MemoryPool memory(nullptr);
  sql::ExecutionContext exec_ctx(&memory, csv_scan->GetOutputSchema(), &callback);

  // Run & Check
  auto query = CompilationContext::Compile(*csv_scan);
  query->Run(&exec_ctx);

  multi_checker.CheckCorrectness();
}

}  // namespace noisepage::execution::sql::codegen::test
#endif
