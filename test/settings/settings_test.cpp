#include <util/test_harness.h>
#include <cstdio>
#include <cstring>
#include <memory>
#include <pqxx/pqxx>  // NOLINT
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "main/db_main.h"
#include "network/connection_handle_factory.h"
#include "settings/settings_manager.h"
#include "storage/garbage_collector.h"
#include "util/transaction_test_util.h"

#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

namespace terrier::settings {

class SettingsTests : public TerrierTest {
 protected:
  DBMain *db_;
  SettingsManager *settings_manager_;
  transaction::TransactionContext *txn_;
  transaction::TransactionManager *txn_manager_;
  catalog::Catalog *catalog_;
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
    std::unordered_map<Param, ParamInfo> param_map;

#define __SETTING_POPULATE__           // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_POPULATE__            // NOLINT

    db_ = new DBMain(std::move(param_map));

    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, nullptr);
    db_->terrier_txn_manager_ = txn_manager_;
    StartGC(txn_manager_);

    txn_ = txn_manager_->BeginTransaction();
    catalog_ = new catalog::Catalog(txn_manager_, txn_);
    settings_manager_ = new SettingsManager(db_, catalog_, txn_manager_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    EndGC();
    TerrierTest::TearDown();
    delete db_;
    delete catalog_;
    delete txn_manager_;
    delete settings_manager_;
  }

  static void EmptySetterCallback(const std::shared_ptr<common::ActionContext> &action_context UNUSED_ATTRIBUTE) {}
};

// NOLINTNEXTLINE
TEST_F(SettingsTests, BasicTest) {
  // Test immutable parameters.
  auto port = static_cast<uint16_t>(settings_manager_->GetInt(Param::port));
  EXPECT_EQ(port, 15721);

  const int32_t action_id = 1;
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(action_id);
  settings_manager_->SetInt(Param::port, 23333, action_context, setter_callback);
  EXPECT_EQ(common::ActionState::FAILURE, action_context->GetState());

  // Test tunable parameters.
  double pi = settings_manager_->GetDouble(Param::pi);
  EXPECT_EQ(pi, 3.14159);
  settings_manager_->SetDouble(Param::pi, 3.14, action_context, setter_callback);
  EXPECT_EQ(3.14, settings_manager_->GetDouble(Param::pi));

  bool parallel = settings_manager_->GetBool(Param::parallel_execution);
  EXPECT_TRUE(parallel);
  settings_manager_->SetBool(Param::parallel_execution, false, action_context, setter_callback);
  EXPECT_FALSE(settings_manager_->GetBool(Param::parallel_execution));

  std::string_view name = settings_manager_->GetString(Param::db_name);
  EXPECT_EQ("Terrier", name);
  settings_manager_->SetString(Param::db_name, "PelotonSP", action_context, setter_callback);
  EXPECT_EQ("PelotonSP", settings_manager_->GetString(Param::db_name));
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, CallbackTest) {
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

// NOLINTNEXTLINE
TEST_F(SettingsTests, ConcurrentModifyTest) {
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;

  std::thread t1([&] {
    std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(1);
    settings_manager_->SetDouble(Param::pi, 3.14, action_context, setter_callback);
    EXPECT_EQ(3.14, settings_manager_->GetDouble(Param::pi));
  });

  std::thread t2([&] {
    std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(2);
    settings_manager_->SetBool(Param::parallel_execution, false, action_context, setter_callback);
    EXPECT_FALSE(settings_manager_->GetBool(Param::parallel_execution));
  });

  std::thread t3([&] {
    std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(3);
    settings_manager_->SetString(Param::db_name, "PelotonSP", action_context, setter_callback);
    EXPECT_EQ("PelotonSP", settings_manager_->GetString(Param::db_name));
  });

  t1.join();
  t2.join();
  t3.join();
}

}  // namespace terrier::settings
