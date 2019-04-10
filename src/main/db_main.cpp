#include <memory>
#include "main/db_main.h"

namespace terrier {
int DBMain::Init(int argc, char **argv) {
  try {
    // init settings manager
    // init gc
    // init catalog
    // init woker pool
    init_main_logger();
    // initialize namespace specific loggers
    terrier::storage::init_index_logger();
    terrier::storage::init_storage_logger();
    terrier::transaction::init_transaction_logger();
    terrier::parser::init_parser_logger();
    terrier::network::init_network_logger();
    // Flush all *registered* loggers using a worker thread.
    // Registered loggers must be thread safe for this to work correctly
    spdlog::flush_every(std::chrono::seconds(DEBUG_LOG_FLUSH_INTERVAL));
  } catch (const spdlog::spdlog_ex &ex) {
    std::cout << "debug log init failed " << ex.what() << std::endl;  // NOLINT
    return 1;
  }
  // log init now complete
  LOG_TRACE("Logger initialization complete");

  // initialize stat registry
  main_stat_reg_ = std::make_shared<terrier::common::StatisticsRegistry>();

  LOG_INFO("Initialization complete");
  return 0;
}

int DBMain::Start() {
  terrier::network::TerrierServer terrier_server;
  terrier_server.SetupServer().ServerLoop();
  return 0;
}

void DBMain::Shutdown() {
  // shutdown loggers
  spdlog::shutdown();
  main_stat_reg_->Shutdown(false);
}
}  // namespace terrier