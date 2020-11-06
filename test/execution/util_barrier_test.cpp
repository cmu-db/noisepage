#include <chrono>  // NOLINT
#include <future>  // NOLINT
#include <vector>

#include "execution/tpl_test.h"
#include "execution/util/barrier.h"

namespace noisepage::execution::util::test {

using namespace std::chrono_literals;  // NOLINT

TEST(BarrierTest, Wait) {
  Barrier barrier(2);
  EXPECT_EQ(0u, barrier.GetGeneration());

  // Spawn one task that sets the flag. But make it wait until we get to the
  // barrier
  std::atomic_bool flag = false;
  auto future_result = std::async([&]() {
    barrier.Wait();
    flag = true;
  });

  // The flag shouldn't be set since we haven't reached the trigger
  EXPECT_FALSE(flag);

  // Sleep for a bit and ensure the flag still isn't set
  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(flag);

  // Set the barrier to let the task through
  barrier.Wait();
  future_result.wait();
  EXPECT_TRUE(flag);
  EXPECT_EQ(1u, barrier.GetGeneration());
}

TEST(BarrierTest, WaitCycle) {
  constexpr uint32_t num_threads = 4;

  std::atomic<uint32_t> count = 0;
  Barrier barrier(num_threads + 1);

  for (uint32_t num_cycles = 0; num_cycles < 5; num_cycles++) {
    count = 0;
    EXPECT_EQ(num_cycles, barrier.GetGeneration());

    // Spawn some threads
    std::vector<std::thread> thread_group;
    for (uint32_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
      thread_group.emplace_back([&]() {
        barrier.Wait();
        count++;
      });
    }

    // At this point, the count should be zero since we haven't reached the
    // barrier
    EXPECT_EQ(0u, count);
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(0u, count);

    // Trigger
    barrier.Wait();

    // Wait
    for (uint32_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
      thread_group[thread_idx].join();
    }

    // Check
    EXPECT_EQ(num_threads, count);
    EXPECT_EQ(num_cycles + 1, barrier.GetGeneration());
  }
}

}  // namespace noisepage::execution::util::test
