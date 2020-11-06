#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <unordered_map>
#include <utility>

#include "gtest/gtest.h"
#include "main/db_main.h"
#include "settings/settings_callbacks.h"
#include "settings/settings_manager.h"
#include "test_util/test_harness.h"

namespace noisepage::settings {

class SettingsTests : public TerrierTest {
 protected:
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<SettingsManager> settings_manager_;
  common::ManagedPointer<storage::LogManager> log_manager_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<storage::RecordBufferSegmentPool> buffer_segment_pool_;

  const uint64_t default_buffer_pool_size_ = 100000;

  void SetUp() override {
    std::unordered_map<Param, ParamInfo> param_map;
    SettingsManager::ConstructParamMap(param_map);

    db_main_ = DBMain::Builder()
                   .SetSettingsParameterMap(std::move(param_map))
                   .SetUseSettingsManager(true)
                   .SetUseLogging(true)
                   .Build();

    settings_manager_ = db_main_->GetSettingsManager();
    log_manager_ = db_main_->GetLogManager();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    buffer_segment_pool_ = db_main_->GetBufferSegmentPool();
  }

  static void EmptySetterCallback(common::ManagedPointer<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}

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
  auto action_context = std::make_unique<common::ActionContext>(action_id);

  // Integer
  auto record_buffer_segment_size = static_cast<uint16_t>(settings_manager_->GetInt(Param::record_buffer_segment_size));
  EXPECT_GE(record_buffer_segment_size, 0);
  settings_manager_->SetInt(Param::record_buffer_segment_size, 9999, common::ManagedPointer(action_context),
                            setter_callback);
  EXPECT_EQ(common::ActionState::SUCCESS, action_context->GetState());
  EXPECT_EQ(static_cast<uint16_t>(settings_manager_->GetInt(Param::record_buffer_segment_size)), 9999);

  // String
  auto log_file_path = static_cast<std::string>(settings_manager_->GetString(Param::wal_file_path));
  EXPECT_FALSE(log_file_path.empty());
  // FIXME: We currently do not have a string parameter that is mutable, so we cannot test
  //        whether we are able to set a string in the SettingsManager yet.
  // settings_manager_->SetString(Param::wal_file_path, "fake.log", action_context, setter_callback);
  // EXPECT_EQ(common::ActionState::SUCCESS, action_context->GetState());
  // EXPECT_EQ(static_cast<std::String>(settings_manager_->GetString(Param::wal_file_path)), "fake.log");

  // TODO(pavlo): Boolean

  // TODO(pavlo): Double
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, ImmutableValueTest) {
  const common::action_id_t action_id(1);
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  auto action_context = std::make_unique<common::ActionContext>(action_id);

  // Test immutable parameters.
  auto port = static_cast<uint16_t>(settings_manager_->GetInt(Param::port));
  EXPECT_EQ(port, 15721);
  try {
    settings_manager_->SetInt(Param::port, 23333, common::ManagedPointer(action_context), setter_callback);
  } catch (SettingsException &e) {
    EXPECT_EQ(e.code_, common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  EXPECT_EQ(common::ActionState::FAILURE, action_context->GetState());
  port = static_cast<uint16_t>(settings_manager_->GetInt(Param::port));
  EXPECT_EQ(port, 15721);
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, InvalidValueTest) {
  const common::action_id_t action_id(1);
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  auto action_context = std::make_unique<common::ActionContext>(action_id);

  // Test setting an invalid parameter using the set API
  auto gc_interval = static_cast<uint32_t>(settings_manager_->GetInt(Param::gc_interval));
  EXPECT_GE(gc_interval, 0);
  try {
    settings_manager_->SetInt(Param::gc_interval, -1, common::ManagedPointer(action_context), setter_callback);
  } catch (SettingsException &e) {
    EXPECT_EQ(e.code_, common::ErrorCode::ERRCODE_INVALID_PARAMETER_VALUE);
  }
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
  auto context0 = std::make_unique<common::ActionContext>(action_id);
  EXPECT_EQ(context0->GetState(), common::ActionState::INITIATED);
  SettingsTests::invoked_ = false;
  auto callback0 = +[](common::ManagedPointer<common::ActionContext> action_context) -> void {
    EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
    SettingsTests::invoked_ = true;
  };
  settings_manager_->SetInt(Param::record_buffer_segment_reuse, 10, common::ManagedPointer(context0), callback0);
  EXPECT_EQ(context0->GetState(), common::ActionState::SUCCESS);
  EXPECT_TRUE(SettingsTests::invoked_);

  // FAILURE
  auto context1 = std::make_unique<common::ActionContext>(action_id);
  EXPECT_EQ(context1->GetState(), common::ActionState::INITIATED);
  SettingsTests::invoked_ = false;
  auto callback1 = +[](common::ManagedPointer<common::ActionContext> action_context) -> void {
    EXPECT_EQ(action_context->GetState(), common::ActionState::FAILURE);
    SettingsTests::invoked_ = true;
  };

  try {
    settings_manager_->SetInt(Param::port, 9999, common::ManagedPointer(context1), callback1);
  } catch (SettingsException &e) {
    EXPECT_EQ(e.code_, common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  EXPECT_EQ(context1->GetState(), common::ActionState::FAILURE);
  EXPECT_TRUE(SettingsTests::invoked_);
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, ParamCallbackTest) {
  // Check that if we set a parameter with a callback that the change gets propagated to the object

  auto buffer_pool_size = static_cast<int64_t>(settings_manager_->GetInt(Param::record_buffer_segment_size));
  EXPECT_EQ(buffer_pool_size, default_buffer_pool_size_);

  buffer_pool_size = buffer_segment_pool_->GetSizeLimit();
  EXPECT_EQ(buffer_pool_size, default_buffer_pool_size_);

  const common::action_id_t action_id(1);
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  auto action_context = std::make_unique<common::ActionContext>(action_id);

  // Setting new value should invoke callback.
  const int64_t new_buffer_pool_size = default_buffer_pool_size_ + 1;
  settings_manager_->SetInt(Param::record_buffer_segment_size, static_cast<int32_t>(new_buffer_pool_size),
                            common::ManagedPointer(action_context), setter_callback);
  buffer_pool_size = static_cast<int64_t>(settings_manager_->GetInt(Param::record_buffer_segment_size));
  EXPECT_EQ(buffer_pool_size, new_buffer_pool_size);

  buffer_pool_size = buffer_segment_pool_->GetSizeLimit();
  EXPECT_EQ(buffer_pool_size, new_buffer_pool_size);
}

// NOLINTNEXTLINE
TEST_F(SettingsTests, LogManagerSettingsTest) {
  const common::action_id_t action_id(1);

  // Check default value is correctly passed to log manager
  auto num_buffers = settings_manager_->GetInt64(Param::wal_num_buffers);
  EXPECT_EQ(num_buffers, log_manager_->TestGetNumBuffers());

  // Change value
  auto new_num_buffers = num_buffers + 1;

  auto action_context = std::make_unique<common::ActionContext>(action_id);
  setter_callback_fn setter_callback = SettingsTests::EmptySetterCallback;
  settings_manager_->SetInt64(Param::wal_num_buffers, new_num_buffers, common::ManagedPointer(action_context),
                              setter_callback);

  // Check new value is propagated
  EXPECT_EQ(new_num_buffers, settings_manager_->GetInt64(Param::wal_num_buffers));
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
          auto action_context = std::make_unique<common::ActionContext>(action_id);
          settings_manager_->SetInt(Param::record_buffer_segment_size, new_size, common::ManagedPointer(action_context),
                                    setter_callback);
          EXPECT_EQ(action_context->GetState(), common::ActionState::SUCCESS);
        },
        i + 1000);
  }

  for (auto &thread : threads) {
    thread.join();
  }

  auto buffer_pool_size_param = static_cast<uint64_t>(settings_manager_->GetInt(Param::record_buffer_segment_size));
  uint64_t buffer_pool_size = buffer_segment_pool_->GetSizeLimit();
  EXPECT_EQ(buffer_pool_size_param, buffer_pool_size);
}

}  // namespace noisepage::settings
