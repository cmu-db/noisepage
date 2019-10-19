#include "common/rusage_monitor.h"
#include "common/macros.h"
#include "storage/storage_defs.h"
#include "util/test_harness.h"

namespace terrier {

// NOLINTNEXTLINE
TEST(RusageMonitorTests, BasicTest) {
  common::RusageMonitor monitor;
  volatile uint64_t j = 0;
  const uint64_t num_iters = 1e8;

  auto workload = [&](uint64_t i) { j = i * 2; };

  monitor.Start();
  for (uint64_t i = 0; i < num_iters; i++) {
    workload(i);
    EXPECT_EQ(j, i * 2);
  }
  monitor.Stop();

  const auto usage = monitor.Usage();

  EXPECT_GT(common::RusageMonitor::TimevalToMicroseconds(usage.ru_utime), 0);
}

}  // namespace terrier
