#include "common/thread_cpu_timer.h"
#include <iostream>
#include "gtest/gtest.h"

namespace terrier {

/**
 * This is a bit difficult to test due to some non-determinism from thread scheduling, CPU frequency scaling, etc.
 * The idea is to have a simple task and run it for some number of iterations while timing it. Then, scale the number of
 * iterations by 10 and then 100. At the end, we expect the CPU time spent on this to scale roughly linearly with the
 * iterations, but this is difficult to guarantee so we just use EXPECT_LT.
 */
// NOLINTNEXTLINE
TEST(ThreadCPUTimerTests, BasicTest) {
  common::ThreadCPUTimer timer;
  volatile uint64_t j;
  const uint64_t num_iters = 100000;

  timer.Start();
  j = 0;
  for (uint64_t i = 0; i < num_iters; i++) {
    j = i * 2;
    EXPECT_EQ(j, i * 2);
  }
  timer.Stop();
  const auto elapsed_time_1 = timer.ElapsedTime();

  timer.Start();
  j = 0;
  for (uint64_t i = 0; i < num_iters * 10; i++) {
    j = i * 2;
    EXPECT_EQ(j, i * 2);
  }
  timer.Stop();
  const auto elapsed_time_2 = timer.ElapsedTime();

  timer.Start();
  j = 0;
  for (uint64_t i = 0; i < num_iters * 100; i++) {
    j = i * 2;
    EXPECT_EQ(j, i * 2);
  }
  timer.Stop();
  const auto elapsed_time_3 = timer.ElapsedTime();

  EXPECT_LT(elapsed_time_1.user_time_us_, elapsed_time_2.user_time_us_);
  EXPECT_LT(elapsed_time_2.user_time_us_, elapsed_time_3.user_time_us_);
}

}  // namespace terrier
