#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "execution/execution_util.h"
#include "execution/vm/module.h"
#include "main/db_main.h"
#include "test_util/fs_util.h"
#include "test_util/tpch/workload.h"

/**
 * The local paths to the data directories.
 * https://github.com/malin1993ml/tpl_tables and "bash gen_tpch.sh 0.1".
 */
static constexpr const char TPCH_TABLE_ROOT[] = "/home/turing/dev/tpl-tables/tables/";
static constexpr const char SSB_TABLE_ROOT[] = "/home/turing/dev/tpl-tables/tables/";
static constexpr const char TPCH_DATABASE_NAME[] = "tpch_runner_db";

namespace noisepage::runner {

/**
 * TPCHRunner runs TPCH benchmarks.
 */
class TPCHRunner : public benchmark::Fixture {
 public:
  /** Defines the number of terminals (workers threads) */
  const int8_t total_num_threads_ = 4;

  /** Time (us) to run per terminal (worker thread) */
  const uint64_t execution_us_per_worker_ = 20000000;

  /** The average intervals in microseconds */
  std::vector<uint64_t> avg_interval_us_ = {10, 20, 50, 100, 200, 500, 1000};

  /** The execution mode for the execution engine */
  const execution::vm::ExecutionMode mode_ = execution::vm::ExecutionMode::Interpret;

  /** Flag indicating if only a single test run should be run */
  const bool single_test_run_ = true;

  /** The main database instance */
  std::unique_ptr<DBMain> db_main_;

  /** The workload with loaded data and queries */
  std::unique_ptr<tpch::Workload> workload_;

  /** Local paths to data */
  const std::string tpch_table_root_{TPCH_TABLE_ROOT};
  const std::string ssb_dir_{SSB_TABLE_ROOT};
  const std::string tpch_database_name_{TPCH_DATABASE_NAME};

  /** The benchmark type */
  tpch::Workload::BenchmarkType type_ = tpch::Workload::BenchmarkType::TPCH;

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
BENCHMARK_DEFINE_F(TPCHRunner, Runner)(benchmark::State &state) {
  // Load the TPCH tables and compile the queries
  std::string table_root;
  switch (type_) {
    case tpch::Workload::BenchmarkType::TPCH:
      table_root = tpch_table_root_;
      break;
    case tpch::Workload::BenchmarkType::SSB:
      table_root = ssb_dir_;
      break;
    default:
      UNREACHABLE("Unimplemented Benchmark Type");
  }
  workload_ = std::make_unique<tpch::Workload>(common::ManagedPointer<DBMain>(db_main_), tpch_database_name_,
                                               table_root, type_);

  int8_t num_thread_start;
  uint32_t query_num_start, repeat_num;
  if (single_test_run_) {
    query_num_start = workload_->GetQueryNum();
    num_thread_start = total_num_threads_;
    repeat_num = 1;
  } else {
    query_num_start = 1;
    num_thread_start = 1;
    repeat_num = 2;
  }

  auto total_query_num = workload_->GetQueryNum() + 1;
  for (uint32_t query_num = query_num_start; query_num < total_query_num; query_num += 4) {
    for (auto num_threads = num_thread_start; num_threads <= total_num_threads_; num_threads += 3) {
      for (uint32_t repeat = 0; repeat < repeat_num; ++repeat) {
        for (auto avg_interval_us : avg_interval_us_) {
          // Let GC clean up
          std::this_thread::sleep_for(std::chrono::seconds(2));

          common::WorkerPool thread_pool{static_cast<uint32_t>(num_threads), {}};
          thread_pool.Startup();

          for (int8_t i = 0; i < num_threads; i++) {
            thread_pool.SubmitTask([this, i, avg_interval_us, query_num] {
              workload_->Execute(i, execution_us_per_worker_, avg_interval_us, query_num, mode_);
            });
          }

          thread_pool.WaitUntilAllFinished();
          thread_pool.Shutdown();
        }
      }
    }
  }

  // Free the workload here so we don't need to use the loggers anymore
  workload_.reset();
}

BENCHMARK_REGISTER_F(TPCHRunner, Runner)->Unit(benchmark::kMillisecond)->UseManualTime()->Iterations(1);

}  // namespace noisepage::runner
