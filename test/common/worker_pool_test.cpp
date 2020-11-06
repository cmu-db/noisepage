#include "common/worker_pool.h"

#include <atomic>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"
#include "test_util/multithread_test_util.h"
#include "test_util/random_test_util.h"

namespace noisepage {

// Rather minimalistic checks for whether we reuse memory
// NOLINTNEXTLINE
TEST(WorkerPoolTests, BasicTest) {
  common::TaskQueue tasks;
  common::WorkerPool thread_pool(5, tasks);
  thread_pool.Startup();
  std::atomic<int> counter(0);

  int var1 = 1;
  int var2 = 2;
  int var3 = 3;
  int var4 = 4;
  int var5 = 5;
  thread_pool.SubmitTask([&]() {
    var1++;
    counter.fetch_add(1);
  });
  thread_pool.SubmitTask([&]() {
    var2--;
    counter.fetch_add(1);
  });
  thread_pool.SubmitTask([&]() {
    var3 *= var3;
    counter.fetch_add(1);
  });
  thread_pool.SubmitTask([&]() {
    var4 = var4 / var4;
    counter.fetch_add(1);
  });

  thread_pool.SubmitTask([&]() {
    var5 = var5 / var5;
    counter.fetch_add(1);
  });

  // Wait for all the test to finish
  while (counter.load() != 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_EQ(2, var1);
  EXPECT_EQ(1, var2);
  EXPECT_EQ(9, var3);
  EXPECT_EQ(1, var4);
  EXPECT_EQ(1, var5);

  thread_pool.Shutdown();
}

// NOLINTNEXTLINE
TEST(WorkerPoolTests, MoreTest) {
  common::TaskQueue tasks;
  common::WorkerPool thread_pool(5, tasks);
  thread_pool.Startup();
  uint32_t iteration = 10;
  std::default_random_engine generator;
  std::uniform_int_distribution<uint32_t> num_thread{1, MultiThreadTestUtil::HardwareConcurrency()};
  for (uint32_t it = 0; it < iteration; it++) {
    auto workload = [](uint32_t /*unused*/) { std::this_thread::sleep_for(std::chrono::milliseconds(200)); };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_thread(generator), workload);
  }
  thread_pool.Shutdown();
}
}  // namespace noisepage
