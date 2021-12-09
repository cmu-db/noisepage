#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "execution/execution_util.h"
#include "execution/vm/module.h"
#include "main/db_main.h"
#include "test_util/fs_util.h"
#include "test_util/procbench/workload.h"

/**
 * The local paths to the data directories.
 * https://github.com/malin1993ml/tpl_tables and "bash gen_tpch.sh 0.1".
 */
static constexpr const char PROCBENCH_TABLE_ROOT[] = "/home/turing/dev/tpl-tables/tpcds-tables/";
static constexpr const char PROCBENCH_DATABASE_NAME[] = "procbench_runner_db";

namespace noisepage::runner {

/**
 * ProcbenchRunner runs SQL ProcBench benchmarks.
 */
class ProcbenchRunner : public benchmark::Fixture {
 public:
  /** The execution mode for the execution engine */
  const execution::vm::ExecutionMode exec_mode_ = execution::vm::ExecutionMode::Interpret;

  /** The main database instance */
  std::unique_ptr<DBMain> db_main_;

  /** The workload with loaded data and queries */
  std::unique_ptr<procbench::Workload> workload_;

  /** Local paths to data */
  const std::string procbench_table_root_{PROCBENCH_TABLE_ROOT};
  const std::string procbench_database_name_{PROCBENCH_DATABASE_NAME};

  /** Setup the database instance for benchmark. */
  void SetUp(const benchmark::State &state) final {
    auto db_main_builder = DBMain::Builder()
                               .SetUseGC(true)
                               .SetUseCatalog(true)
                               .SetUseGCThread(true)
                               .SetUseMetrics(true)
                               .SetUseMetricsThread(true)
                               .SetBlockStoreSize(1000000)
                               .SetBlockStoreReuse(1000000)
                               .SetRecordBufferSegmentSize(1000000)
                               .SetRecordBufferSegmentReuse(1000000)
                               .SetBytecodeHandlersPath(common::GetBinaryArtifactPath("bytecode_handlers_ir.bc"));
    db_main_ = db_main_builder.Build();

    auto metrics_manager = db_main_->GetMetricsManager();
    metrics_manager->SetMetricSampleRate(metrics::MetricsComponent::EXECUTION_PIPELINE, 100);
    metrics_manager->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE);
  }

  /** Teardown the database instance after a benchmark */
  void TearDown(const benchmark::State &state) final {
    // free db main here so we don't need to use the loggers anymore
    db_main_.reset();
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ProcbenchRunner, Runner)(benchmark::State &state) {
  // Load the ProcBench tables and compile the queries
  workload_ = std::make_unique<procbench::Workload>(common::ManagedPointer<DBMain>(db_main_), procbench_database_name_,
                                                    procbench_table_root_);

  const auto start = std::chrono::high_resolution_clock::now();

  // Execute the workload
  workload_->Execute(6, execution::vm::ExecutionMode::Interpret);

  const auto stop = std::chrono::high_resolution_clock::now();
  const auto duration = std::chrono::duration_cast<std::chrono::seconds>(stop - start).count();

  state.SetIterationTime(duration);

  // Free the workload here so we don't need to use the loggers anymore
  workload_.reset();
}

BENCHMARK_REGISTER_F(ProcbenchRunner, Runner)->Unit(benchmark::kMillisecond)->UseManualTime()->Iterations(1);

}  // namespace noisepage::runner
