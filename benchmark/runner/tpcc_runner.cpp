#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "execution/execution_util.h"
#include "execution/vm/module.h"
#include "main/db_main.h"
#include "test_util/fs_util.h"
#include "test_util/tpcc/workload_cached.h"

namespace noisepage::runner {
// Modified from TPCHRunner
class TPCCRunner : public benchmark::Fixture {
 public:
  const int8_t num_threads_ = 1;                         // defines the number of terminals (workers threads)
  const uint32_t num_precomputed_txns_per_worker_ = 10;  // Number of txns to run per terminal (worker thread)
  const execution::vm::ExecutionMode mode_ = execution::vm::ExecutionMode::Compiled;

  std::unique_ptr<DBMain> db_main_;
  std::unique_ptr<tpcc::WorkloadCached> tpcc_workload_;
  common::WorkerPool thread_pool_{static_cast<uint32_t>(num_threads_), {}};

  // TPCC setup
  const std::vector<std::string> tpcc_txn_names_ = {
      "delivery"  //, "index_scan", "neworder", "orderstatus", "payment",
                  // "seq_scan", "stocklevel", "temp"
  };

  void SetUp(const benchmark::State &state) final {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    settings::SettingsManager::ConstructParamMap(param_map);
    auto db_main_builder = DBMain::Builder()
                               .SetUseGC(true)
                               .SetSettingsParameterMap(std::move(param_map))
                               .SetUseCatalog(true)
                               .SetUseGCThread(true)
                               .SetUseMetrics(true)
                               .SetUseMetricsThread(true)
                               .SetBlockStoreSize(1000)
                               .SetBlockStoreReuse(1000)
                               .SetRecordBufferSegmentSize(1000000)
                               .SetRecordBufferSegmentReuse(1000000)
                               .SetUseSettingsManager(true)
                               .SetUseStatsStorage(true)
                               .SetBytecodeHandlersPath(common::GetBinaryArtifactPath("bytecode_handlers_ir.bc"));

    db_main_ = db_main_builder.Build();

    auto metrics_manager = db_main_->GetMetricsManager();
    metrics_manager->SetMetricSampleRate(metrics::MetricsComponent::EXECUTION, 100);
    metrics_manager->EnableMetric(metrics::MetricsComponent::EXECUTION);
    metrics_manager->SetMetricSampleRate(metrics::MetricsComponent::GARBAGECOLLECTION, 100);
    metrics_manager->EnableMetric(metrics::MetricsComponent::GARBAGECOLLECTION);
    metrics_manager->SetMetricSampleRate(metrics::MetricsComponent::LOGGING, 100);
    metrics_manager->EnableMetric(metrics::MetricsComponent::LOGGING);
  }

  void TearDown(const benchmark::State &state) final {
    // free db main here so we don't need to use the loggers anymore
    db_main_.reset();
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCRunner, Runner)(benchmark::State &state) {
  // Load the TPCC tables and compile the queries
  tpcc_workload_ =
      std::make_unique<tpcc::WorkloadCached>(common::ManagedPointer<DBMain>(db_main_), tpcc_txn_names_, num_threads_);
  std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

  // NOLINTNEXTLINE
  for (auto _ : state) {
    thread_pool_.Startup();

    // run the TPCC workload to completion, timing the execution
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (int8_t i = 0; i < num_threads_; i++) {
        thread_pool_.SubmitTask([this, i] { tpcc_workload_->Execute(i, num_precomputed_txns_per_worker_, mode_); });
      }
      thread_pool_.WaitUntilAllFinished();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    thread_pool_.Shutdown();
  }

  state.SetItemsProcessed(state.iterations() * num_precomputed_txns_per_worker_ * num_threads_);

  // free the workload here so we don't need to use the loggers anymore
  tpcc_workload_.reset();
}

BENCHMARK_REGISTER_F(TPCCRunner, Runner)->Unit(benchmark::kMillisecond)->UseManualTime()->Iterations(1);

}  // namespace noisepage::runner
