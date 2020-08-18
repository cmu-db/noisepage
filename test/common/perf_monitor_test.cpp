#include "common/perf_monitor.h"

#include <thread>  //NOLINT

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "common/scoped_timer.h"
#include "main/db_main.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"

namespace terrier {

class PerfMonitorTests : public TerrierTest {
 public:
  static void CreateAndDestroyCatalog(common::PerfMonitor::PerfCounters *const counters) {
    common::PerfMonitor monitor(false);
    monitor.Start();

    auto db_main = terrier::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
    db_main.reset();

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
