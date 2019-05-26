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
  storage::RecordBufferSegmentPool *buffer_segment_pool_;

  const uint64_t defaultBufferPoolSize = 100000;

  void SetUp() override {
    std::unordered_map<Param, ParamInfo> param_map;
    terrier::settings::SettingsManager::ConstructParamMap(param_map);

    db_main_ = new DBMain(std::move(param_map));
    db_main_->Init();
    settings_manager_ = db_main_->settings_manager_;
    txn_manager_ = db_main_->txn_manager_;
    buffer_segment_pool_ = db_main_->buffer_segment_pool_;
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
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, CallbackTest) {
  auto bufferPoolSize = static_cast<int64_t>(settings_manager_->GetInt(Param::record_buffer_segment_size));
  EXPECT_EQ(bufferPoolSize, defaultBufferPoolSize);

  bufferPoolSize = buffer_segment_pool_->GetSizeLimit();
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

  bufferPoolSize = buffer_segment_pool_->GetSizeLimit();
  EXPECT_EQ(bufferPoolSize, newBufferPoolSize);
}

// Test concurrent modification to buffer pool size.
// NOLINTNEXTLINE
TEST_F(SettingsTests, ConcurrentModifyTest) {
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
  uint64_t bufferPoolSize = buffer_segment_pool_->GetSizeLimit();
  EXPECT_EQ(bufferPoolSizeParam, bufferPoolSize);
}

}  // namespace terrier::settings
