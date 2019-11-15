#include "common/perf_monitor.h"
#include <thread>  //NOLINT
#include "catalog/catalog.h"
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "storage/garbage_collector_thread.h"
#include "storage/storage_defs.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

namespace terrier {

class PerfMonitorTests : public TerrierTest {
 public:
  void SetUp() final { TerrierTest::SetUp(); }

  void TearDown() final { TerrierTest::TearDown(); }

  static void CreateAndDestroyCatalog(common::PerfMonitor::PerfCounters *const counters) {
    common::PerfMonitor monitor(false);
    monitor.Start();

    storage::BlockStore block_store{1000, 1000};
    storage::RecordBufferSegmentPool buffer_pool{1000000, 1000000};

    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager(&timestamp_manager);
    transaction::TransactionManager txn_manager(&timestamp_manager, &deferred_action_manager, &buffer_pool, true,
                                                DISABLED);
    catalog::Catalog catalog(&txn_manager, &block_store);

    storage::GarbageCollector gc(&timestamp_manager, &deferred_action_manager, &txn_manager, DISABLED);
    StorageTestUtil::FullyPerformGC(&gc, DISABLED);

    catalog.TearDown();
    StorageTestUtil::FullyPerformGC(&gc, DISABLED);
    monitor.Stop();
    *counters = monitor.Counters();
  }

  static void JustSleep(common::PerfMonitor::PerfCounters *const counters) {
    common::PerfMonitor monitor(false);
    monitor.Start();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    monitor.Stop();
    *counters = monitor.Counters();
  }
};

/**
 * Simple test that spins off 2 threads. One thread builds and then destroys a Catalog. The other sleeps. The parent
 * waits. We then do comparisons on their respective perf counters based on the work performed.
 */
// NOLINTNEXTLINE
TEST_F(PerfMonitorTests, BasicTest) {
  common::PerfMonitor parent_monitor(false);
  common::PerfMonitor::PerfCounters parent_counters, catalog_counters, sleep_counters;
  parent_monitor.Start();
  std::thread thread1(CreateAndDestroyCatalog, &catalog_counters);
  std::thread thread2(JustSleep, &sleep_counters);
  thread1.join();
  thread2.join();
  parent_monitor.Stop();
  parent_counters = parent_monitor.Counters();

  EXPECT_TRUE(catalog_counters.cpu_cycles_ >= parent_counters.cpu_cycles_);
  EXPECT_TRUE(catalog_counters.instructions_ >= parent_counters.instructions_);
  EXPECT_TRUE(catalog_counters.cache_references_ >= parent_counters.cache_references_);
  EXPECT_TRUE(catalog_counters.bus_cycles_ >= parent_counters.bus_cycles_);
  EXPECT_TRUE(catalog_counters.ref_cpu_cycles_ >= parent_counters.ref_cpu_cycles_);
  EXPECT_TRUE(parent_counters.cpu_cycles_ >= sleep_counters.cpu_cycles_);
  EXPECT_TRUE(parent_counters.instructions_ >= sleep_counters.instructions_);
  EXPECT_TRUE(parent_counters.cache_references_ >= sleep_counters.cache_references_);
  EXPECT_TRUE(parent_counters.bus_cycles_ >= sleep_counters.bus_cycles_);
  EXPECT_TRUE(parent_counters.ref_cpu_cycles_ >= sleep_counters.ref_cpu_cycles_);
}

/**
 * Simple test that spins off 2 threads. One thread builds and then destroys a Catalog. The other sleeps. The parent
 * waits. We then do comparisons on their respective perf counters based on the work performed. This scenario has the
 * parent accumulate for sub-tasks.
 */
// NOLINTNEXTLINE
TEST_F(PerfMonitorTests, InheritTest) {
  common::PerfMonitor parent_monitor(true);
  common::PerfMonitor::PerfCounters parent_counters, catalog_counters, sleep_counters;
  parent_monitor.Start();
  std::thread thread1(CreateAndDestroyCatalog, &catalog_counters);
  std::thread thread2(JustSleep, &sleep_counters);
  thread1.join();
  thread2.join();
  parent_monitor.Stop();
  parent_counters = parent_monitor.Counters();

  EXPECT_TRUE(parent_counters.cpu_cycles_ >= catalog_counters.cpu_cycles_);
  EXPECT_TRUE(parent_counters.instructions_ >= catalog_counters.instructions_);
  EXPECT_TRUE(parent_counters.cache_references_ >= catalog_counters.cache_references_);
  EXPECT_TRUE(parent_counters.bus_cycles_ >= catalog_counters.bus_cycles_);
  EXPECT_TRUE(parent_counters.ref_cpu_cycles_ >= catalog_counters.ref_cpu_cycles_);
  EXPECT_TRUE(parent_counters.cpu_cycles_ >= sleep_counters.cpu_cycles_);
  EXPECT_TRUE(parent_counters.instructions_ >= sleep_counters.instructions_);
  EXPECT_TRUE(parent_counters.cache_references_ >= sleep_counters.cache_references_);
  EXPECT_TRUE(parent_counters.bus_cycles_ >= sleep_counters.bus_cycles_);
  EXPECT_TRUE(parent_counters.ref_cpu_cycles_ >= sleep_counters.ref_cpu_cycles_);
}

}  // namespace terrier
