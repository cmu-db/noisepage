#include <vector>

#include "execution/sql_test.h"
#include "execution/sql/table_vector_iterator.h"

#include "benchmark/benchmark.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "storage/garbage_collector_thread.h"
#include "../../util/include/execution/table_generator/table_generator.h"

namespace terrier::execution {

class SeqscanBenchmark : public benchmark::Fixture, public SqlBasedTest {
  void SetUp(state& st) override {
      // Create test tables
      benchmark::Fixture::SetUp(state& st);
      exec_ctx_ = MakeExecCtx();
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
//  // Pause timer while populating table
//  state.PauseTiming();
////  // Populate table?
////  // execution context?, block_store_, catalogns_oid?
////  execution::sql::TableGenerator();
//  state.ResumeTiming();

   // Where is the tpl.cpp header file?
  // Run TPL file
  execution::RunFile("vec-filter.tpl");

  // can set iteration time and total items processed count but not time for entire scan
}

BENCHMARK_REGISTER_F(SeqscanBenchmark, SequentialScan)->UseManualTime()->Unit(benchmark::kMillisecond);
} // namespace terrier