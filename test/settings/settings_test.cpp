#include <util/test_harness.h>
#include <cstdio>
#include <cstring>
#include <memory>
#include <pqxx/pqxx>  // NOLINT
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
  SettingsManager *settings_manager_;
  transaction::TransactionContext *txn_;
  transaction::TransactionManager *txn_manager_;
  const uint64_t defaultBufferPoolSize = 100000;
  storage::RecordBufferSegmentPool buffer_pool_{defaultBufferPoolSize, 100};
  storage::GarbageCollector *gc_;
  volatile bool run_gc_;
  volatile bool gc_paused_;
  std::thread gc_thread_;
  const std::chrono::milliseconds gc_period_{10};

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      if (!gc_paused_) gc_->PerformGarbageCollection();
    }
  }

  void StartGC(transaction::TransactionManager *const txn_manager) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([this] { GCThreadLoop(); });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  void SetUp() override {
    TerrierTest::SetUp();

    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, nullptr);
    StartGC(txn_manager_);

    txn_ = txn_manager_->BeginTransaction();
    terrier::catalog::terrier_catalog = std::make_shared<terrier::catalog::Catalog>(txn_manager_, txn_);
    settings_manager_ = new SettingsManager(terrier::catalog::terrier_catalog.get(), txn_manager_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    EndGC();
    TerrierTest::TearDown();
    delete txn_manager_;
    delete settings_manager_;
  }

  static void EmptySetterCallback(std::shared_ptr<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}
};

// NOLINTNEXTLINE
TEST_F(SettingsTests, BasicTest) {
  auto port = static_cast<uint16_t>(settings_manager_->GetInt(Param::port));
  EXPECT_EQ(port, 15721);

  const int32_t action_id = 1;
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(action_id);
  EXPECT_THROW(settings_manager_->SetInt(Param::port, 23333, action_context, setter_callback), SettingsException);
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, CallbackTest) {
  MainDatabase::txn_manager_ = txn_manager_;
  auto bufferPoolSize = static_cast<int64_t>(settings_manager_->GetInt(Param::buffer_pool_size));
  EXPECT_EQ(bufferPoolSize, defaultBufferPoolSize);

  bufferPoolSize = txn_manager_->GetBufferPoolSizeLimit();
  EXPECT_EQ(bufferPoolSize, defaultBufferPoolSize);

  const int32_t action_id = 1;
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(action_id);

  // Setting new value should invoke callback.
  const int64_t newBufferPoolSize = defaultBufferPoolSize + 1;
  settings_manager_->SetInt(Param::buffer_pool_size, static_cast<int32_t>(newBufferPoolSize), action_context,
                            setter_callback);
  bufferPoolSize = static_cast<int64_t>(settings_manager_->GetInt(Param::buffer_pool_size));
  EXPECT_EQ(bufferPoolSize, newBufferPoolSize);

  bufferPoolSize = txn_manager_->GetBufferPoolSizeLimit();
  EXPECT_EQ(bufferPoolSize, newBufferPoolSize);
}

}  // namespace terrier::settings
