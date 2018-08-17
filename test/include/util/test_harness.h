
#pragma once

#include "common/main_stat_registry.h"
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"

namespace terrier {

class TerrierTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // initialize loggers

    init_main_logger();
    // initialize namespace specific loggers
    terrier::storage::init_storage_logger();
    terrier::transaction::init_transaction_logger();

    // initialize main statistics registry
    init_main_stat_reg();
  }

  void TearDown() override {
    // shutdown loggers
    spdlog::shutdown();
  }
};

}  // namespace terrier
