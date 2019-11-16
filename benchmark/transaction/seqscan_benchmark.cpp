#include <vector>

#include "execution/sql_test.h"
#include "execution/sql/table_vector_iterator.h"

#include "benchmark/benchmark.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "storage/garbage_collector_thread.h"
#include "../../util/include/execution/table_generator/table_generator.h"

namespace terrier::execution {

class SeqscanBenchmark : public benchmark::Fixture, public SqlBasedTest {
  // TODO: Fix disagreement between SetUp function argument types
  void SetUp(State& st) {
    // Run setup file
    SqlBasedTest::SetUp();
    // Create execution context
    exec_ctx_ = MakeExecCtx();
    // Create test tables
    GenerateTestTables(exec_ctx_.get());
  }

 public:
 parser::ConstantValueExpression DummyExpr() {
   return parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0));
 }

 protected:
 /**
  * Execution context to use for the test
  */
 std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};


  /** 
   * Run a sequential scan through the LLVM engine without accessing the SQLTable
   */
  // NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SeqscanBenchmark, SequentialScan)(benchmark::State &state) {
  // TODO: Import main function from tpl.cpp and run with command line options
  // TODO: Determine path to tpl.cpp and vec-filter.tpl

  // Pause timer while running compiler
  state.PauseTiming();
  system("gcc -o a.out tpl.cpp vec-filter.tpl");
  state.ResumeTiming();

  // Determine time to run input tpl file
  system("./a.out");
}

BENCHMARK_REGISTER_F(SeqscanBenchmark, SequentialScan)->UseManualTime()->Unit(benchmark::kMillisecond);
} // namespace terrier