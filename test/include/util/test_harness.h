
#pragma once

#include <memory>
#include "common/main_stat_registry.h"
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"

std::shared_ptr<terrier::common::StatisticsRegistry> main_stat_reg;

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
    main_stat_reg = std::make_shared<common::StatisticsRegistry>();
  }

  void TearDown() override {
    // shutdown loggers
    spdlog::shutdown();
    // shutdown main statistics registry
    main_stat_reg->Shutdown(false);
  }
};

}  // namespace terrier
