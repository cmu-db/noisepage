#include "main/db_main.h"
#include <memory>

namespace terrier {

int DBMain::Init(int argc, char **argv) {
  try {
    init_main_logger();
    // initialize namespace specific loggers
    storage::init_index_logger();
    storage::init_storage_logger();
    transaction::init_transaction_logger();
    parser::init_parser_logger();
    network::init_network_logger();
    // Flush all *registered* loggers using a worker thread.
    // Registered loggers must be thread safe for this to work correctly
    spdlog::flush_every(std::chrono::seconds(DEBUG_LOG_FLUSH_INTERVAL));
  } catch (const spdlog::spdlog_ex &ex) {
    std::cout << "debug log init failed " << ex.what() << std::endl;  // NOLINT
    return 1;
  }
  LOG_TRACE("Logger initialization complete");

  // TODO(Weichen): init settings manager
  // init gc
  // init catalog
  // init worker pool

  main_stat_reg_ = std::make_shared<common::StatisticsRegistry>();

  LOG_INFO("Initialization complete");
  return 0;
}

int DBMain::Start() {
  // TODO(Weichen): Boot Traffic cop here
  terrier_server_.SetupServer().ServerLoop();
  return 0;
}

void DBMain::Shutdown() {
  terrier_server_.Close();

  main_stat_reg_->Shutdown(false);

  LOG_INFO("Terrier has shut down.");

  // shutdown loggers
  spdlog::shutdown();
}

}  // namespace terrier
