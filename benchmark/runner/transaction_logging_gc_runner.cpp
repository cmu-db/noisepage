#include <string>
#include <vector>
#include "benchmark/benchmark.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "common/dedicated_thread_registry.h"
#include "common/scoped_timer.h"
#include "metrics/metrics_thread.h"
#include "storage/garbage_collector_thread.h"

#define LOG_FILE_NAME "benchmark.txt"

namespace terrier::runner {

class TransactionLoggingGCRunner : public benchmark::Fixture {
 public:
  const std::vector<uint16_t> attr_sizes_ = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
  const uint32_t initial_table_size_ = 1000000;
  std::default_random_engine generator_;
  storage::LogManager *log_manager_ = nullptr;
  storage::GarbageCollector *gc_ = nullptr;
  storage::GarbageCollectorThread *gc_thread_ = nullptr;
  const std::chrono::microseconds gc_period_{1000};
  const std::chrono::microseconds metrics_period_{10000};
  common::DedicatedThreadRegistry *thread_registry_ = nullptr;

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::microseconds log_serialization_interval_{100};
  const std::chrono::microseconds log_persist_interval_{100};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TransactionLoggingGCRunner, Runner)(benchmark::State &state) {
  const auto txn_length = static_cast<uint32_t>(state.range(0));
  const auto txn_interval = static_cast<uint32_t>(state.range(1));
  const auto num_thread = static_cast<uint32_t>(state.range(2));
  const double insert = static_cast<double>(state.range(3)) / 100;
  const double update = static_cast<double>(state.range(4)) / 100;
  const double select = static_cast<double>(state.range(5)) / 100;

  // scale up num_txns by the number of threads since it counts for all threads
  const uint32_t num_txns = (log2(num_thread) + 1) * 2000000 / txn_interval;

  uint64_t abort_count = 0;
  const std::vector<double> insert_update_select_ratio = {insert, update, select};

  storage::BlockStore block_store{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool{100000000, 100000000};

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto *const metrics_manager = new metrics::MetricsManager();
    auto *const metrics_thread = new metrics::MetricsThread(common::ManagedPointer(metrics_manager), metrics_period_);
    metrics_manager->EnableMetric(metrics::MetricsComponent::TRANSACTION, 500);
    metrics_manager->EnableMetric(metrics::MetricsComponent::LOGGING, 0);
    metrics_manager->EnableMetric(metrics::MetricsComponent::GARBAGECOLLECTION, 0);

    thread_registry_ = new common::DedicatedThreadRegistry(common::ManagedPointer(metrics_manager));

    log_manager_ =
        new storage::LogManager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                                log_persist_threshold_, common::ManagedPointer(&buffer_pool),
                                common::ManagedPointer<common::DedicatedThreadRegistry>(thread_registry_));
    log_manager_->Start();

    LargeDataTableBenchmarkObject tested(attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio,
                                         &block_store, &buffer_pool, &generator_, true, log_manager_);
    // log all of the Inserts from table creation
    log_manager_->ForceFlush();

    gc_ = new storage::GarbageCollector(common::ManagedPointer(tested.GetTimestampManager()), DISABLED,
                                        common::ManagedPointer(tested.GetTxnManager()), DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(gc_), gc_period_,
                                                     common::ManagedPointer(metrics_manager));
    const auto result = tested.SimulateOltp(num_txns, num_thread, metrics_manager, txn_interval);
    abort_count += result.first;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      log_manager_->ForceFlush();
    }
    state.SetIterationTime(static_cast<double>(result.second + elapsed_ms) / 1000.0);
    log_manager_->PersistAndStop();
    delete log_manager_;
    delete gc_thread_;
    delete thread_registry_;
    delete metrics_thread;
    unlink(LOG_FILE_NAME);
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

static void UNUSED_ATTRIBUTE EnumeratedArguments(benchmark::internal::Benchmark *b) {
  std::vector<uint32_t> txn_lengths = {1, 2, 4};
  // submit interval between two transactions (us)
  std::vector<uint32_t> txn_intervals = {1, 5, 10, 50, 100};
  std::vector<uint32_t> num_threads = {4, 2, 1, 8, 12, 16};

  for (uint32_t txn_length : txn_lengths)
    for (uint32_t txn_interval : txn_intervals)
      for (uint32_t num_thread : num_threads)
        for (uint32_t insert = 0; insert <= 10; insert += 10)
          for (uint32_t update = 0; update <= 100 - insert; update += 30) {
            b->Args({txn_length, txn_interval, num_thread, insert, update, 100 - insert - update});
          }
}

BENCHMARK_REGISTER_F(TransactionLoggingGCRunner, Runner)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->Iterations(1)
    ->Apply(EnumeratedArguments);
}  // namespace terrier::runner
