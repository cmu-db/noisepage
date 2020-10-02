#pragma once

#include <memory>
#include <thread>  // NOLINT
#include <vector>

#include "execution/util/barrier.h"
#include "execution/util/cpu_info.h"
#include "execution/util/execution_common.h"
#include "execution/util/timer.h"
#include "gtest/gtest.h"
#include "loggers/execution_logger.h"
#include "test_util/test_harness.h"

namespace terrier::execution {

class TplTest : public terrier::TerrierTest {
 public:
  TplTest() { CpuInfo::Instance(); }

  const char *GetTestName() const { return ::testing::UnitTest::GetInstance()->current_test_info()->name(); }
};

template <typename F>
static double Bench(uint32_t repeat, const F &f) {
  if (repeat > 4) {
    // Warmup
    f();
    repeat--;
  }

  util::Timer<std::milli> timer;
  timer.Start();

  for (uint32_t i = 0; i < repeat; i++) {
    f();
  }

  timer.Stop();
  return timer.GetElapsed() / static_cast<double>(repeat);
}

template <typename F>
static void LaunchParallel(uint32_t num_threads, const F &f) {
  util::Barrier barrier(num_threads + 1);

  std::vector<std::thread> thread_group;

  for (uint32_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
    thread_group.emplace_back(
        [&](auto tid) {
          barrier.Wait();
          f(tid);
        },
        thread_idx);
  }

  barrier.Wait();

  for (uint32_t i = 0; i < num_threads; i++) {
    thread_group[i].join();
  }
}

}  // namespace terrier::execution
