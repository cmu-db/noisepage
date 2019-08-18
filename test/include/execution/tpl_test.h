#pragma once

#include "execution/util/cpu_info.h"
#include "execution/util/execution_common.h"
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

}  // namespace terrier::execution
