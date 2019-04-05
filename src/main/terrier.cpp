#include <fstream>
#include <iostream>
#include <memory>
#include <vector>
#include "bwtree/bwtree.h"
#include "common/allocator.h"
#include "common/settings.h"
#include "common/stat_registry.h"
#include "common/strong_typedef.h"
#include "loggers/index_logger.h"
#include "loggers/main_logger.h"
#include "loggers/network_logger.h"
#include "loggers/parser_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"
#include "loggers/type_logger.h"
#include "network/connection_handle_factory.h"
#include "network/terrier_server.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "traffic_cop/traffic_cop.h"
#include "transaction/transaction_context.h"

int main() {
  // initialize loggers
  try {
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
  auto main_stat_reg = std::make_shared<terrier::common::StatisticsRegistry>();

  LOG_INFO("Initialization complete");

  terrier::network::TerrierServer terrier_server;
  terrier::network::TrafficCopPtr t_cop(new terrier::traffic_cop::TrafficCop());
  terrier::network::ConnectionHandleFactory::GetInstance().SetTrafficCop(t_cop);
  terrier_server.SetPort(terrier::common::Settings::SERVER_PORT);
  terrier_server.SetupServer().ServerLoop();

  // shutdown loggers
  spdlog::shutdown();
  main_stat_reg->Shutdown(false);
}
