#pragma once

#include "execution/util/execution_common.h"
#include "execution/util/cpu_info.h"
#include "execution/util/timer.h"
#include "gtest/gtest.h"
#include "loggers/execution_logger.h"
#include "util/test_harness.h"

namespace terrier::execution {

class TplTest : public terrier::TerrierTest {
 public:
  TplTest() { CpuInfo::Instance(); }

  void SetUp() override { terrier::TerrierTest::SetUp(); }
};

template <typename F>
static inline double Bench(uint32_t repeat, const F &f) {
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
  return timer.elapsed() / static_cast<double>(repeat);
}

}  // namespace terrier::execution
