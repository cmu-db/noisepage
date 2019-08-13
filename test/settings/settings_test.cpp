#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <unordered_map>
#include <utility>
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "settings/settings_callbacks.h"
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
  storage::LogManager *log_manager_;
  transaction::TransactionManager *txn_manager_;
  storage::RecordBufferSegmentPool *buffer_segment_pool_;

  const uint64_t defaultBufferPoolSize = 100000;

  void SetUp() override {
    std::unordered_map<Param, ParamInfo> param_map;
    terrier::settings::SettingsManager::ConstructParamMap(param_map);

    db_main_ = new DBMain(std::move(param_map));
    settings_manager_ = db_main_->settings_manager_;
    log_manager_ = db_main_->log_manager_;
    txn_manager_ = db_main_->txn_manager_;
    buffer_segment_pool_ = db_main_->buffer_segment_pool_;
  }

  void TearDown() override { delete db_main_; }

  static void EmptySetterCallback(const std::shared_ptr<common::ActionContext> &action_context UNUSED_ATTRIBUTE) {}

  /**
   * This is a hacky placeholder variable that we can use in lambda callbacks
   * If you use this in the tests below, then you have to make sure that reset it before
   * you use it.
   */
  inline static bool invoked_;  // NOLINT
};

// NOLINTNEXTLINE
TEST_F(SettingsTests, BasicTest) {
  const common::action_id_t action_id(1);
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(action_id);

  // Integer
  auto record_buffer_segment_size = static_cast<uint16_t>(settings_manager_->GetInt(Param::record_buffer_segment_size));
  EXPECT_GE(record_buffer_segment_size, 0);
  settings_manager_->SetInt(Param::record_buffer_segment_size, 9999, action_context, setter_callback);
  EXPECT_EQ(common::ActionState::SUCCESS, action_context->GetState());
  EXPECT_EQ(static_cast<uint16_t>(settings_manager_->GetInt(Param::record_buffer_segment_size)), 9999);

  // String
  auto log_file_path = static_cast<std::string>(settings_manager_->GetString(Param::log_file_path));
  EXPECT_FALSE(log_file_path.empty());
  // FIXME: We currently do not have a string parameter that is mutable, so we cannot test
  //        whether we are able to set a string in the SettingsManager yet.
  // settings_manager_->SetString(Param::log_file_path, "fake.log", action_context, setter_callback);
  // EXPECT_EQ(common::ActionState::SUCCESS, action_context->GetState());
  // EXPECT_EQ(static_cast<std::String>(settings_manager_->GetString(Param::log_file_path)), "fake.log");

  // TODO(pavlo): Boolean

  // TODO(pavlo): Double
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, ImmutableValueTest) {
  const common::action_id_t action_id(1);
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(action_id);

  // Test immutable parameters.
  auto port = static_cast<uint16_t>(settings_manager_->GetInt(Param::port));
  EXPECT_EQ(port, 15721);
  settings_manager_->SetInt(Param::port, 23333, action_context, setter_callback);
  EXPECT_EQ(common::ActionState::FAILURE, action_context->GetState());
  port = static_cast<uint16_t>(settings_manager_->GetInt(Param::port));
  EXPECT_EQ(port, 15721);
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, InvalidValueTest) {
  const common::action_id_t action_id(1);
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(action_id);

  // Test setting an invalid parameter using the set API
  auto gc_interval = static_cast<uint32_t>(settings_manager_->GetInt(Param::gc_interval));
  EXPECT_GE(gc_interval, 0);
  settings_manager_->SetInt(Param::gc_interval, -1, action_context, setter_callback);
  EXPECT_EQ(common::ActionState::FAILURE, action_context->GetState());
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, SetterCallbackTest) {
  // Invoke a setter callbacks to make sure that our callbacks get invoked correctly
  // and that our ActionContexts have the proper state set.
  // Note that the SetterCallback is *always* invoked regardless of whether
  // the param update is successful or not.

  const common::action_id_t action_id(1);

  // SUCCESS
  std::shared_ptr<common::ActionContext> context0 = std::make_shared<common::ActionContext>(action_id);
  EXPECT_EQ(context0->GetState(), common::ActionState::INITIATED);
  SettingsTests::invoked_ = false;
  auto callback0 = +[](const std::shared_ptr<common::ActionContext> &action_context) -> void {
    EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
    SettingsTests::invoked_ = true;
  };
  settings_manager_->SetInt(Param::record_buffer_segment_reuse, 10, context0, callback0);
  EXPECT_EQ(context0->GetState(), common::ActionState::SUCCESS);
  EXPECT_TRUE(SettingsTests::invoked_);

  // FAILURE
  std::shared_ptr<common::ActionContext> context1 = std::make_shared<common::ActionContext>(action_id);
  EXPECT_EQ(context1->GetState(), common::ActionState::INITIATED);
  SettingsTests::invoked_ = false;
  auto callback1 = +[](const std::shared_ptr<common::ActionContext> &action_context) -> void {
    EXPECT_EQ(action_context->GetState(), common::ActionState::FAILURE);
    SettingsTests::invoked_ = true;
  };
  settings_manager_->SetInt(Param::port, 9999, context1, callback1);
  EXPECT_EQ(context1->GetState(), common::ActionState::FAILURE);
  EXPECT_TRUE(SettingsTests::invoked_);
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, ParamCallbackTest) {
  // Check that if we set a parameter with a callback that the change gets propagated to the object

  auto bufferPoolSize = static_cast<int64_t>(settings_manager_->GetInt(Param::record_buffer_segment_size));
  EXPECT_EQ(bufferPoolSize, defaultBufferPoolSize);

  bufferPoolSize = buffer_segment_pool_->GetSizeLimit();
  EXPECT_EQ(bufferPoolSize, defaultBufferPoolSize);

  const common::action_id_t action_id(1);
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

// NOLINTNEXTLINE
TEST_F(SettingsTests, LogManagerSettingsTest) {
  const common::action_id_t action_id(1);

  // Check default value is correctly passed to log manager
  auto num_buffers = settings_manager_->GetInt(Param::num_log_manager_buffers);
  EXPECT_EQ(num_buffers, log_manager_->TestGetNumBuffers());

  // Change value
  auto new_num_buffers = num_buffers + 1;

  std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(action_id);
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  settings_manager_->SetInt(Param::num_log_manager_buffers, new_num_buffers, action_context, setter_callback);

  // Check new value is propagated
  EXPECT_EQ(new_num_buffers, settings_manager_->GetInt(Param::num_log_manager_buffers));
  EXPECT_EQ(new_num_buffers, log_manager_->TestGetNumBuffers());
}

// Test concurrent modification to buffer pool size.
// NOLINTNEXTLINE
TEST_F(SettingsTests, ConcurrentModifyTest) {
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  const common::action_id_t action_id(1);

  const int nthreads = 16;
  std::thread threads[nthreads];
  for (int i = 0; i < nthreads; i++) {
    threads[i] = std::thread(
        [&](int new_size) {
          std::shared_ptr<common::ActionContext> action_context = std::make_shared<common::ActionContext>(action_id);
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
