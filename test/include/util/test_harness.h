#pragma once

#include <memory>
#include "gtest/gtest.h"
#include "loggers/catalog_logger.h"
#include "loggers/index_logger.h"
#include "loggers/main_logger.h"
#include "loggers/network_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/test_logger.h"
#include "loggers/transaction_logger.h"
#include "loggers/type_logger.h"

namespace terrier {

class TerrierTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // initialize loggers

    init_main_logger();
    // initialize namespace specific loggers
    terrier::storage::init_index_logger();
    terrier::network::init_network_logger();
    terrier::storage::init_storage_logger();
    terrier::transaction::init_transaction_logger();
    terrier::catalog::init_catalog_logger();

    // only needed in the test framework.
    init_test_logger();
  }

  void TearDown() override {
    // shutdown loggers
    spdlog::shutdown();
  }
};

}  // namespace terrier
