#include <network/terrier_server.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <vector>
#include "bwtree/bwtree.h"
#include "catalog/catalog.h"
#include "common/allocator.h"
#include "common/stat_registry.h"
#include "common/strong_typedef.h"
#include "loggers/catalog_logger.h"
#include "loggers/index_logger.h"
#include "loggers/main_logger.h"
#include "loggers/network_logger.h"
#include "loggers/parser_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"
#include "loggers/type_logger.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
int main() {
  // initialize loggers
  try {
    init_main_logger();
    // initialize namespace specific loggers
    terrier::storage::init_index_logger();
    terrier::storage::init_storage_logger();
    terrier::transaction::init_transaction_logger();
    terrier::catalog::init_catalog_logger();
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

  // create the global transaction mgr
  terrier::storage::RecordBufferSegmentPool buffer_pool_(100000, 10000);
  terrier::transaction::TransactionManager txn_manager_(&buffer_pool_, true, nullptr);
  terrier::transaction::TransactionContext *txn_ = txn_manager_.BeginTransaction();
  // create the (system) catalogs
  terrier::catalog::terrier_catalog = std::make_shared<terrier::catalog::Catalog>(&txn_manager_, txn_);
  LOG_INFO("Initialization complete");

  terrier::network::TerrierServer terrier_server;
  terrier_server.SetupServer().ServerLoop();

  // TODO(pakhtar): fix so the catalog works nicely with the GC, and shutdown is clean and leak-free. (#323)
  // shutdown loggers
  spdlog::shutdown();
  main_stat_reg->Shutdown(false);
}
