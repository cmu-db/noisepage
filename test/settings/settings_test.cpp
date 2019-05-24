#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <unordered_map>
#include <utility>
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "settings/settings_manager.h"
#include "util/test_harness.h"

#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

namespace terrier::settings {

class SettingsTests : public TerrierTest {
 protected:
  DBMain *db_main_;
  SettingsManager *settings_manager_;
  transaction::TransactionManager *txn_manager_;
  const uint64_t defaultBufferPoolSize = 100000;

  void SetUp() override {
    std::unordered_map<Param, ParamInfo> param_map;
    terrier::settings::SettingsManager::ConstructParamMap(param_map);

    db_main_ = new DBMain(std::move(param_map));
    db_main_->Init();
    settings_manager_ = db_main_->settings_manager_;
    txn_manager_ = db_main_->txn_manager_;
  }

  void TearDown() override { delete db_main_; }

  static void EmptySetterCallback(const std::shared_ptr<common::ActionContext> &action_context UNUSED_ATTRIBUTE) {}
};

// NOLINTNEXTLINE
TEST_F(SettingsTests, BasicTest) {
  const int32_t action_id = 1;
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(action_id);

  // Test immutable parameters.
  auto port = static_cast<uint16_t>(settings_manager_->GetInt(Param::port));
  EXPECT_EQ(port, 15721);
  settings_manager_->SetInt(Param::port, 23333, action_context, setter_callback);
  EXPECT_EQ(common::ActionState::FAILURE, action_context->GetState());

  settings_manager_->SetBool(Param::fixed_bool, true, action_context, setter_callback);
  EXPECT_EQ(common::ActionState::FAILURE, action_context->GetState());

  settings_manager_->SetDouble(Param::fixed_double, 100.0, action_context, setter_callback);
  EXPECT_EQ(common::ActionState::FAILURE, action_context->GetState());

  settings_manager_->SetString(Param::fixed_string, "abcdefg", action_context, setter_callback);
  EXPECT_EQ(common::ActionState::FAILURE, action_context->GetState());

  // Test tunable parameters.
  settings_manager_->SetInt(Param::lucky_number, 1919810, action_context, setter_callback);
  EXPECT_EQ(common::ActionState::FAILURE, action_context->GetState());
  EXPECT_EQ(114, settings_manager_->GetInt(Param::lucky_number));
  settings_manager_->SetInt(Param::lucky_number, 233, action_context, setter_callback);
  EXPECT_EQ(233, settings_manager_->GetInt(Param::lucky_number));

  double pi = settings_manager_->GetDouble(Param::pi);
  EXPECT_EQ(pi, 3.14159);
  settings_manager_->SetDouble(Param::pi, 3.14, action_context, setter_callback);
  EXPECT_EQ(3.14, settings_manager_->GetDouble(Param::pi));
  settings_manager_->SetDouble(Param::pi, 0.0, action_context, setter_callback);
  EXPECT_EQ(common::ActionState::FAILURE, action_context->GetState());

  bool parallel = settings_manager_->GetBool(Param::parallel_execution);
  EXPECT_TRUE(parallel);
  settings_manager_->SetBool(Param::parallel_execution, false, action_context, setter_callback);
  EXPECT_FALSE(settings_manager_->GetBool(Param::parallel_execution));

  std::string name = settings_manager_->GetString(Param::db_name);
  EXPECT_EQ("Terrier", name);
  settings_manager_->SetString(Param::db_name, "TerrierSP", action_context, setter_callback);
  EXPECT_EQ("TerrierSP", settings_manager_->GetString(Param::db_name));
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, CallbackTest) {
  auto bufferPoolSize = static_cast<int64_t>(settings_manager_->GetInt(Param::record_buffer_segment_size));
  EXPECT_EQ(bufferPoolSize, defaultBufferPoolSize);

  bufferPoolSize = txn_manager_->GetBufferPoolSizeLimit();
  EXPECT_EQ(bufferPoolSize, defaultBufferPoolSize);

  const int32_t action_id = 1;
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(action_id);

  // Setting new value should invoke callback.
  const int64_t newBufferPoolSize = defaultBufferPoolSize + 1;
  settings_manager_->SetInt(Param::record_buffer_segment_size, static_cast<int32_t>(newBufferPoolSize), action_context,
                            setter_callback);
  bufferPoolSize = static_cast<int64_t>(settings_manager_->GetInt(Param::record_buffer_segment_size));
  EXPECT_EQ(bufferPoolSize, newBufferPoolSize);

  bufferPoolSize = txn_manager_->GetBufferPoolSizeLimit();
  EXPECT_EQ(bufferPoolSize, newBufferPoolSize);
}

// Test concurrent modification to different parameters.
// NOLINTNEXTLINE
TEST_F(SettingsTests, ConcurrentModifyTest1) {
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
    settings_manager_->SetString(Param::db_name, "TerrierSP", action_context, setter_callback);
    EXPECT_EQ("TerrierSP", settings_manager_->GetString(Param::db_name));
  });

  t1.join();
  t2.join();
  t3.join();
}

// Test concurrent modification to buffer pool size.
// NOLINTNEXTLINE
TEST_F(SettingsTests, ConcurrentModifyTest2) {
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;

  const int nthreads = 16;
  std::thread threads[nthreads];
  for (int i = 0; i < nthreads; i++) {
    threads[i] = std::thread(
        [&](int new_size) {
          std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(1);
          settings_manager_->SetInt(Param::record_buffer_segment_size, new_size, action_context, setter_callback);
          EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
        },
        i + 1000);
  }

  for (auto &thread : threads) {
    thread.join();
  }

  auto bufferPoolSizeParam = static_cast<uint64_t>(settings_manager_->GetInt(Param::record_buffer_segment_size));
  uint64_t bufferPoolSize = txn_manager_->GetBufferPoolSizeLimit();
  EXPECT_EQ(bufferPoolSizeParam, bufferPoolSize);
}

}  // namespace terrier::settings
