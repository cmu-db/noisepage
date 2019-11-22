#include <vector>

#include "execution/sql/table_vector_iterator.h"
#include "execution/sql_test.h"

#include "../../util/include/execution/table_generator/table_generator.h"
#include "benchmark/benchmark.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "storage/garbage_collector_thread.h"

namespace terrier::execution {

class SeqscanBenchmark : public benchmark::Fixture {
  // TODO: Fix disagreement between SetUp function argument types * do I use SqlBasedTest class? *
//  void SetUp(benchmark::State &st) {
//
//  }
//
// public:
//  parser::ConstantValueExpression DummyExpr() {
//    return parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0));
//  }
//
// protected:
//  /**
//   * Execution context to use for the test
//   */
//  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

/**
 * Run a sequential scan through the LLVM engine without accessing the SQLTable
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SeqscanBenchmark, SequentialScan)(benchmark::State &state) {
  // TODO: Import main function from tpl.cpp and run with command line options
  // TODO: Determine path to tpl.cpp and vec-filter.tpl

  // Pause timer while compling file
  state.PauseTiming();
  const char *cmd = "gcc -std=c++17 -g -ggdb -O0 -Wall -Werror -o a.out ../../util/execution/tpl.cpp ../../sample_tpl/vec-filter.cpp";
  system(cmd);
  state.ResumeTiming();

  // Determine time to run input tpl file
  system("./a.out");
}

BENCHMARK_REGISTER_F(SeqscanBenchmark, SequentialScan)->UseManualTime()->Unit(benchmark::kMillisecond);
}  // namespace terrier::execution