#pragma once

#include <memory>
#include "gtest/gtest.h"
#include "loggers/loggers_util.h"

namespace terrier {

class TerrierTest : public ::testing::Test {
 protected:
  void SetUp() override { LoggersUtil::Initialize(true); }

  void TearDown() override { LoggersUtil::ShutDown(); }
};

}  // namespace terrier
