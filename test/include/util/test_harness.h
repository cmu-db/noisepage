//
// Created by pakhtar on 8/15/18.
//

#pragma once
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "loggers/storage_logger.h"

namespace terrier {
namespace test {

class TerrierTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // initialize loggers

    init_main_logger();
    // initialize namespace specific loggers
    ::terrier::storage::init_storage_logger();
  }

  void TearDown() override {
    // shutdown loggers
    spdlog::shutdown();
  }
};

}  // namespace test
}  // namespace terrier
