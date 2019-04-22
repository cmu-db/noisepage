#include "main/db_main.h"
#include <memory>
#include "loggers/index_logger.h"
#include "loggers/parser_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"

namespace terrier {

void DBMain::InitLoggers() {
  try {
    init_main_logger();
    // initialize namespace specific loggers
    storage::init_index_logger();
    storage::init_storage_logger();
    transaction::init_transaction_logger();
    parser::init_parser_logger();
    network::init_network_logger();

    // Flush all *registered* loggers using a background thread.
    // Registered loggers must be thread safe for this to work correctly
    spdlog::flush_every(std::chrono::seconds(DEBUG_LOG_FLUSH_INTERVAL));
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "debug log init failed " << ex.what() << std::endl;  // NOLINT
    throw ex;
  }
  LOG_TRACE("Logger initialization complete");
}

void DBMain::Init() {
  InitLoggers();

  // TODO(Weichen): init settings manager
  // init gc
  // init catalog
  // init worker pool
  main_stat_reg_ = std::make_shared<common::StatisticsRegistry>();

  LOG_INFO("Initialization complete");
}

void DBMain::Run() {
  // TODO(Weichen): Boot Traffic cop here
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

}  // namespace terrier
