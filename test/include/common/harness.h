#pragma once

#include "common/logger.h"

#include "gtest/gtest.h"

class TerrierTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // turn on logger
    terrier::Logger::InitializeLogger();
  }

  virtual void TearDown() {}
};
