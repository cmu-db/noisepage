#include "main/db_main.h"
#include <memory>
#include "loggers/loggers_util.h"
#include "settings/settings_manager.h"

namespace terrier {

void DBMain::Init() {
  LoggersUtil::Initialize(false);

  // initialize stat registry
  main_stat_reg_ = std::make_shared<common::StatisticsRegistry>();

  // create the global transaction mgr
  auto *buffer_pool = new storage::RecordBufferSegmentPool(
      type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::buffer_pool_size)->second.value_),
      10000);
  txn_manager_ = new transaction::TransactionManager(buffer_pool, true, nullptr);
  transaction::TransactionContext *txn_ = txn_manager_->BeginTransaction();
  // create the (system) catalogs
  catalog_ = new catalog::Catalog(txn_manager_, txn_);
  settings_manager_ = new settings::SettingsManager(this, catalog_);
  LOG_INFO("Initialization complete");
}

void DBMain::Run() {
  server_.SetPort(static_cast<int16_t>(
      type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::port)->second.value_)));
  server_.SetupServer().ServerLoop();

  // server loop exited, begin cleaning up
  CleanUp();
}

void DBMain::ForceShutdown() {
  server_.Close();
  CleanUp();
}

void DBMain::CleanUp() {
  main_stat_reg_->Shutdown(false);
  LOG_INFO("Terrier has shut down.");

  LoggersUtil::ShutDown();
}

void DBMain::EmptyCallback(void *old_value, void *new_value,
                           const std::shared_ptr<common::ActionContext> &action_context) {
  action_context->SetState(common::ActionState::SUCCESS);
}

void DBMain::BufferPoolSizeCallback(void *old_value, void *new_value,
                                    const std::shared_ptr<common::ActionContext> &action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int new_size = *static_cast<int *>(new_value);
  bool success = txn_manager_->SetBufferPoolSizeLimit(new_size);
  if (success)
    action_context->SetState(common::ActionState::SUCCESS);
  else
    action_context->SetState(common::ActionState::FAILURE);
}

}  // namespace terrier
