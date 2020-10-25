#pragma once
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "loggers/loggers_util.h"

namespace noisepage {

class TerrierTest : public ::testing::Test {
 public:
  TerrierTest() { LoggersUtil::Initialize(); }

  ~TerrierTest() override { LoggersUtil::ShutDown(); }
};

}  // namespace noisepage
