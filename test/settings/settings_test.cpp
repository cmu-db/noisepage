#include <util/test_harness.h>
#include <cstdio>
#include <cstring>
#include <memory>
#include <pqxx/pqxx>
#include <string>
#include <unordered_map>
#include <vector>
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "main/main_database.h"
#include "network/connection_handle_factory.h"
#include "settings/settings_manager.h"
#include "storage/garbage_collector.h"
#include "util/transaction_test_util.h"

namespace terrier::settings {

class SettingsTests : public TerrierTest {
 protected:
  std::shared_ptr<SettingsManager> settings_manager_;
  storage::RecordBufferSegmentPool *buffer_pool_;
  transaction::TransactionContext *txn_;
  transaction::TransactionManager *txn_manager_;
  const int64_t defaultBufferPoolSize = 100000;
  void SetUp() override {
    TerrierTest::SetUp();

    buffer_pool_ = new storage::RecordBufferSegmentPool(defaultBufferPoolSize, 10000);
    txn_manager_ = new transaction::TransactionManager(buffer_pool_, false, nullptr);
    txn_ = txn_manager_->BeginTransaction();
    terrier::catalog::terrier_catalog = std::make_shared<terrier::catalog::Catalog>(txn_manager_, txn_);
    settings_manager_ = std::make_shared<SettingsManager>(terrier::catalog::terrier_catalog, txn_manager_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    TerrierTest::TearDown();
    delete buffer_pool_;
    delete txn_manager_;
    delete txn_;
  }
};

// NOLINTNEXTLINE
TEST_F(SettingsTests, BasicTest) {
  auto port = static_cast<uint16_t>(settings_manager_->GetInt(Param::port));
  EXPECT_EQ(port, 15721);

  EXPECT_THROW(settings_manager_->SetInt(Param::port, 23333), SettingsException);
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, CallbackTest) {
  MainDatabase::txn_manager_ = txn_manager_;
  auto bufferPoolSize = static_cast<int64_t>(settings_manager_->GetInt(Param::buffer_pool_size));
  EXPECT_EQ(bufferPoolSize, defaultBufferPoolSize);

  bufferPoolSize = txn_manager_->GetBufferPoolSizeLimit();
  EXPECT_EQ(bufferPoolSize, defaultBufferPoolSize);

  // Setting new value should invoke callback.
  const int64_t newBufferPoolSize = defaultBufferPoolSize + 1;
  settings_manager_->SetInt(Param::buffer_pool_size, static_cast<int32_t>(newBufferPoolSize));
  bufferPoolSize = static_cast<int64_t>(settings_manager_->GetInt(Param::buffer_pool_size));
  EXPECT_EQ(bufferPoolSize, newBufferPoolSize);

  bufferPoolSize = txn_manager_->GetBufferPoolSizeLimit();
  EXPECT_EQ(bufferPoolSize, newBufferPoolSize);
}

}  // namespace terrier::settings
