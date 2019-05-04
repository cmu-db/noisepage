#include "main/db_main.h"
#include <memory>
#include "loggers/catalog_logger.h"
#include "loggers/index_logger.h"
#include "loggers/network_logger.h"
#include "loggers/parser_logger.h"
#include "loggers/settings_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"
#include "loggers/type_logger.h"
#include "settings/settings_manager.h"

namespace terrier {
void DBMain::InitLoggers() {
  try {
    init_main_logger();
    // initialize namespace specific loggers
    storage::init_index_logger();
    storage::init_storage_logger();
    transaction::init_transaction_logger();
    catalog::init_catalog_logger();
    settings::init_settings_logger();
    parser::init_parser_logger();
    network::init_network_logger();
    // Flush all *registered* loggers using a worker thread.
    // Registered loggers must be thread safe for this to work correctly
    spdlog::flush_every(std::chrono::seconds(DEBUG_LOG_FLUSH_INTERVAL));
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "debug log init failed " << ex.what() << std::endl;  // NOLINT
    throw ex;
  }
  // log init now complete
  LOG_TRACE("Logger initialization complete");
}

void DBMain::Init() {
  InitLoggers();

  // initialize stat registry
  main_stat_reg_ = std::make_shared<terrier::common::StatisticsRegistry>();

  // create the global transaction mgr
  terrier::storage::RecordBufferSegmentPool buffer_pool_(100000, 10000);
  terrier_txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, nullptr);
  terrier::transaction::TransactionContext *txn_ = terrier_txn_manager_->BeginTransaction();
  // create the (system) catalogs
  terrier_catalog_ = new terrier::catalog::Catalog(terrier_txn_manager_, txn_);
  terrier_settings_manager_ = new settings::SettingsManager(this, terrier_catalog_, terrier_txn_manager_);
  LOG_INFO("Initialization complete");
}

void DBMain::Run() {
  terrier_server_.SetPort(static_cast<int16_t>(terrier_settings_manager_->GetInt(terrier::settings::Param::port)));
  terrier_server_.SetupServer().ServerLoop();

  // server loop exited, begin cleaning up
  CleanUp();
}

void DBMain::ForceShutdown() {
  terrier_server_.Close();
  CleanUp();
}

void DBMain::CleanUp() {
  main_stat_reg_->Shutdown(false);
  LOG_INFO("Terrier has shut down.");

  spdlog::shutdown();
}

void DBMain::EmptyCallback(void *old_value, void *new_value, std::shared_ptr<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::SUCCESS);
}

void DBMain::BufferPoolSizeCallback(void *old_value, void *new_value,
                                    std::shared_ptr<common::ActionContext> action_context) {
  action_context->SetState(common::ActionState::IN_PROGRESS);
  int new_size = *static_cast<int *>(new_value);
  bool success = terrier_txn_manager_->SetBufferPoolSizeLimit(new_size);
  if (success)
    action_context->SetState(common::ActionState::SUCCESS);
  else
    action_context->SetState(common::ActionState::FAILURE);
}

}  // namespace terrier
