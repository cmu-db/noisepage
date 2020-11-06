#include "common/perf_monitor.h"

#include <thread>  //NOLINT

#include "common/managed_pointer.h"
#include "main/db_main.h"
#include "test_util/test_harness.h"

namespace noisepage {

/**
 * These tests mostly exist to make sure we can compile and use the API of perf counters. It's difficult to make any
 * assertions about the counters' values due to OS behavior (unsupported on macOS, unsupported on Linux without changing
 * kernel flags or possibly running the tests as root). I keep the test around as an easy sanity check via break points
 * to make sure perf counters give us the data we want on a given system.
 */
class PerfMonitorTests : public TerrierTest {};

template <bool inherit, typename perf_counters>
static void CreateAndDestroyCatalog(perf_counters *const counters) {
  common::PerfMonitor<inherit> monitor;
  monitor.Start();

  auto db_main = noisepage::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
  db_main.reset();

  monitor.Stop();
  *counters = monitor.Counters();
}

template <bool inherit, typename perf_counters>
static void JustSleep(perf_counters *const counters) {
  common::PerfMonitor<inherit> monitor;
  monitor.Start();

  std::this_thread::sleep_for(std::chrono::seconds(2));

  monitor.Stop();
  *counters = monitor.Counters();
}

/**
 * Simple test that spins off 2 threads. One thread builds and then destroys a Catalog. The other sleeps. The parent
 * waits. We can't make strong assertions about their values due to scheduling uncertainty.
 */
template <bool inherit, typename perf_counters>
static void UnbalancedChildrenThreads() {
  common::PerfMonitor<inherit> parent_monitor;
  perf_counters parent_counters, catalog_counters, sleep_counters;
  parent_monitor.Start();
  std::thread thread1(CreateAndDestroyCatalog<inherit, perf_counters>, &catalog_counters);
  std::thread thread2(JustSleep<inherit, perf_counters>, &sleep_counters);
  thread1.join();
  thread2.join();
  parent_monitor.Stop();
  parent_counters = parent_monitor.Counters();
}

/**
 * Test with inherit flag (count children) false
 */
// NOLINTNEXTLINE
TEST_F(PerfMonitorTests, BasicTest) {
  constexpr bool inherit = false;
  UnbalancedChildrenThreads<inherit, common::PerfMonitor<inherit>::PerfCounters>();
}

/**
 * Test with inherit flag (count children) true
 */
// NOLINTNEXTLINE
TEST_F(PerfMonitorTests, InheritTest) {
  constexpr bool inherit = true;
  UnbalancedChildrenThreads<inherit, common::PerfMonitor<inherit>::PerfCounters>();
}

}  // namespace noisepage
