#include "common/rusage_monitor.h"

#include <thread>  //NOLINT

#include "common/macros.h"
#include "storage/storage_defs.h"
#include "test_util/test_harness.h"

namespace noisepage {

/**
 * Simple test that just does some work wrapped in a RusageMonitor and checks the user time afterwards. It should be
 * greated than 0 since we did work.
 */
// NOLINTNEXTLINE
TEST(RusageMonitorTests, BasicTest) {
  common::RusageMonitor monitor(true);
  uint64_t j = 0;
  const uint64_t num_iters = 1e8;

  monitor.Start();
  for (uint64_t i = 0; i < num_iters; i++) {
    j = i * 2;
    EXPECT_EQ(j, i * 2);
  }
  monitor.Stop();

  const auto usage = monitor.Usage();

  EXPECT_GT(common::RusageMonitor::TimevalToMicroseconds(usage.ru_utime), 0);
}

/**
 * Spin up a couple of threads, monitor thread-level and process-level counters, compare at the end.
 */
// NOLINTNEXTLINE
TEST(RusageMonitorTests, ThreadTest) {
  common::RusageMonitor process_monitor(true), thread1_monitor(false), thread2_monitor(false);
  const uint64_t num_iters = 1e8;

  auto workload = [](common::RusageMonitor *const thread_monitor) {
    thread_monitor->Start();
    uint64_t j = 0;
    for (uint64_t i = 0; i < num_iters; i++) {
      j = i * 2;
      EXPECT_EQ(j, i * 2);
    }
    thread_monitor->Stop();
  };

  process_monitor.Start();
  std::thread thread1(workload, &thread1_monitor), thread2(workload, &thread2_monitor);
  thread1.join();
  thread2.join();
  process_monitor.Stop();

  const auto process_cpu_time_us = common::RusageMonitor::TimevalToMicroseconds(process_monitor.Usage().ru_utime);
  const auto thread1_cpu_time_us = common::RusageMonitor::TimevalToMicroseconds(thread1_monitor.Usage().ru_utime);
  const auto thread2_cpu_time_us = common::RusageMonitor::TimevalToMicroseconds(thread2_monitor.Usage().ru_utime);

  EXPECT_GT(process_cpu_time_us, thread1_cpu_time_us);
  EXPECT_GT(process_cpu_time_us, thread2_cpu_time_us);
}

}  // namespace noisepage
