#include <common/dedicated_thread_registry.h>
#include <common/execution_thread_pool.h>
#include <common/managed_pointer.h>

#include <atomic>
#include <thread>  // NOLINT
#include <vector>

#include "common/worker_pool.h"
#include "gtest/gtest.h"
#include "test_util/multithread_test_util.h"
#include "test_util/random_test_util.h"

namespace terrier {

// Rather minimalistic checks for whether we reuse memory
// NOLINTNEXTLINE
TEST(ExecutionThreadPoolTests, SimpleTest) {
  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids;
  for (int i = 0; i < static_cast<int>(std::thread::hardware_concurrency()); i++) {
    cpu_ids.emplace_back(i);
  }
  common::ExecutionThreadPool thread_pool(common::ManagedPointer(&registry), &cpu_ids);
  std::atomic<int> counter(0);

  int var1 = 1;

  std::promise<void> p;
  thread_pool.SubmitTask(&p, [&]() {
    var1++;
    counter.fetch_add(1);
  });

  // Wait for all the test to finish
  p.get_future().get();

  EXPECT_EQ(2, var1);

}

// NOLINTNEXTLINE
TEST(ExecutionThreadPoolTests, BasicTest) {
  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids;
  for (int i = 0; i < static_cast<int>(std::thread::hardware_concurrency()); i++) {
    cpu_ids.emplace_back(i);
  }
  common::ExecutionThreadPool thread_pool(common::ManagedPointer(&registry), &cpu_ids);
  std::atomic<int> counter(0);

  int var1 = 1;
  int var2 = 2;
  int var3 = 3;
  int var4 = 4;
  int var5 = 5;
  std::promise<void> ps[5];
  thread_pool.SubmitTask(&ps[0], [&]() {
    var1++;
    counter.fetch_add(1);
  });
  thread_pool.SubmitTask(&ps[1], [&]() {
    var2--;
    counter.fetch_add(1);
  });
  thread_pool.SubmitTask(&ps[2], [&]() {
    var3 *= var3;
    counter.fetch_add(1);
  });
  thread_pool.SubmitTask(&ps[3], [&]() {
    var4 = var4 / var4;
    counter.fetch_add(1);
  });

  thread_pool.SubmitTask(&ps[4], [&]() {
    var5 = var5 / var5;
    counter.fetch_add(1);
  });

  for (int i = 0; i < 5; i++)
    ps[i].get_future().get();

  EXPECT_EQ(2, var1);
  EXPECT_EQ(1, var2);
  EXPECT_EQ(9, var3);
  EXPECT_EQ(1, var4);
  EXPECT_EQ(1, var5);

}

// NOLINTNEXTLINE
TEST(ExecutionThreadPoolTests, MoreTest) {
  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids;
  for (int i = 0; i < static_cast<int>(std::thread::hardware_concurrency()); i++) {
    cpu_ids.emplace_back(i);
  }
  common::ExecutionThreadPool thread_pool(common::ManagedPointer(&registry), &cpu_ids);
  uint32_t iteration = 10;
  std::default_random_engine generator;
  std::uniform_int_distribution<uint32_t> num_thread{1, MultiThreadTestUtil::HardwareConcurrency()};
  for (uint32_t it = 0; it < iteration; it++) {
    auto workload = [] { std::this_thread::sleep_for(std::chrono::milliseconds(200)); };

    uint32_t num_threads_used = num_thread(generator);
    std::promise<void> promises[num_threads_used];
    for (uint32_t i = 0; i < num_threads_used; i++) {
      thread_pool.SubmitTask(&promises[i], workload);
    }

    for (uint32_t i = 0; i < num_threads_used; i++) {
      promises[i].get_future().get();
    }
  }
}
}  // namespace terrier
