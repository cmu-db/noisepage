#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "execution/execution_util.h"
#include "execution/vm/module.h"
#include "main/db_main.h"
#include "test_util/tpch/workload.h"

namespace noisepage::runner {
class TPCHRunner : public benchmark::Fixture {
 public:
  const int8_t total_num_threads_ = 4;                // defines the number of terminals (workers threads)
  const uint64_t execution_us_per_worker_ = 1000000;  // Time (us) to run per terminal (worker thread)
  std::vector<uint64_t> avg_interval_us_ = {10, 20, 50, 100, 200, 500, 1000};
  const execution::vm::ExecutionMode mode_ = execution::vm::ExecutionMode::Interpret;

  std::unique_ptr<DBMain> db_main_;
  std::unique_ptr<tpch::Workload> workload_;

  // To get tpl_tables, https://github.com/malin1993ml/tpl_tables and "bash gen_tpch.sh 0.1".
  const std::string tpch_table_root_ = "../../../tpl_tables/tables/";
  const std::string ssb_dir_ = "../../../SSB_Table_Generator/ssb_tables/";
  const std::string tpch_database_name_ = "benchmark_db";

  tpch::Workload::BenchmarkType type_ = tpch::Workload::BenchmarkType::TPCH;

  void SetUp(const benchmark::State &state) final {
    noisepage::execution::ExecutionUtil::InitTPL();
    auto db_main_builder = DBMain::Builder()
                               .SetUseGC(true)
                               .SetUseCatalog(true)
                               .SetUseGCThread(true)
                               .SetUseMetrics(true)
                               .SetUseMetricsThread(true)
                               .SetBlockStoreSize(1000000)
                               .SetBlockStoreReuse(1000000)
                               .SetRecordBufferSegmentSize(1000000)
                               .SetRecordBufferSegmentReuse(1000000);
    db_main_ = db_main_builder.Build();

    auto metrics_manager = db_main_->GetMetricsManager();
    metrics_manager->SetMetricSampleInterval(metrics::MetricsComponent::EXECUTION_PIPELINE, 0);
    metrics_manager->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE);
    metrics_manager->SetMetricSampleInterval(metrics::MetricsComponent::GARBAGECOLLECTION, 0);
    metrics_manager->EnableMetric(metrics::MetricsComponent::GARBAGECOLLECTION);
    metrics_manager->SetMetricSampleInterval(metrics::MetricsComponent::LOGGING, 0);
    metrics_manager->EnableMetric(metrics::MetricsComponent::LOGGING);
  }

  void TearDown(const benchmark::State &state) final {
    noisepage::execution::ExecutionUtil::ShutdownTPL();
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

  auto total_query_num = workload_->GetQueryNum() + 1;
  for (uint32_t query_num = 1; query_num < total_query_num; ++query_num)
    for (auto num_threads = 1; num_threads <= total_num_threads_; num_threads += 2)
      for (uint32_t repeat = 0; repeat < 3; ++repeat)
        for (auto avg_interval_us : avg_interval_us_) {
          std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up
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

  // free the workload here so we don't need to use the loggers anymore
  workload_.reset();
}

BENCHMARK_REGISTER_F(TPCHRunner, Runner)->Unit(benchmark::kMillisecond)->UseManualTime()->Iterations(1);
}  // namespace noisepage::runner
