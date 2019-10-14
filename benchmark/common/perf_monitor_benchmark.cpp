#include <random>
#include <string>
#include <thread>
#include <vector>
#include "benchmark/benchmark.h"
#include "catalog/catalog.h"
#include "common/macros.h"
#include "common/perf_monitor.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "metrics/logging_metric.h"
#include "metrics/metrics_thread.h"
#include "storage/garbage_collector_thread.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"
#include "util/tpcc/builder.h"
#include "util/tpcc/database.h"
#include "util/tpcc/loader.h"
#include "util/tpcc/worker.h"
#include "util/tpcc/workload.h"
#include "util/tpcc/util.h"

namespace terrier {

class PerfMonitorBenchmarks : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {}

  void TearDown(const benchmark::State &state) final {}

  static void ThreadWork1() {
    common::PerfMonitor monitor;
    monitor.Start();

    const uint64_t blockstore_size_limit_ =
        1000;  // May need to increase this if num_threads_ or num_precomputed_txns_per_worker_ are greatly increased
    // (table sizes grow with a bigger workload)
    const uint64_t blockstore_reuse_limit_ = 1000;
    const uint64_t buffersegment_size_limit_ = 1000000;
    const uint64_t buffersegment_reuse_limit_ = 1000000;
    storage::BlockStore block_store_{blockstore_size_limit_, blockstore_reuse_limit_};
    storage::RecordBufferSegmentPool buffer_pool_{buffersegment_size_limit_, buffersegment_reuse_limit_};
    std::default_random_engine generator_;

    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager(&timestamp_manager);
    transaction::TransactionManager txn_manager(&timestamp_manager, &deferred_action_manager, &buffer_pool_, true,
                                                DISABLED);
    catalog::Catalog catalog(&txn_manager, &block_store_);

    storage::GarbageCollector gc(&timestamp_manager, &deferred_action_manager, &txn_manager, DISABLED);
    StorageTestUtil::FullyPerformGC(&gc, DISABLED);

    catalog.TearDown();
    StorageTestUtil::FullyPerformGC(&gc, DISABLED);
    monitor.Stop();
    std::cout << "THREAD 1" << std::endl;
    monitor.ReadCounters().Print();
  }

  static void ThreadWork2() {
    common::PerfMonitor monitor;
    monitor.Start();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    monitor.Stop();
    std::cout << "THREAD 2" << std::endl;
    monitor.ReadCounters().Print();
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(PerfMonitorBenchmarks, PerfMonitor)(benchmark::State &state) {
  common::PerfMonitor parent;
  parent.Start();
  // NOLINTNEXTLINE
//  for (auto _ : state) {
    std::thread thread1(ThreadWork1);
    std::thread thread2(ThreadWork2);
    thread1.join();
    thread2.join();
//  }
  parent.Stop();
  std::cout << "PARENT" << std::endl;
  parent.ReadCounters().Print();
//  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(PerfMonitorBenchmarks, PerfMonitor);
}  // namespace terrier
