#include "main/main_database.h"

namespace terrier {
int MainDatabase::start(int argc, char *argv[]) {
  try {
    init_main_logger();
    // initialize namespace specific loggers
    terrier::storage::init_index_logger();
    terrier::storage::init_storage_logger();
    terrier::transaction::init_transaction_logger();
    terrier::catalog::init_catalog_logger();
    terrier::settings::init_settings_logger();
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
  // initialize loggers
  ::google::SetUsageMessage("Usage Info: \n");
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  // initialize stat registry
  auto main_stat_reg = std::make_shared<terrier::common::StatisticsRegistry>();

  // create the global transaction mgr
  terrier::storage::RecordBufferSegmentPool buffer_pool_(100000, 10000);
  terrier::transaction::TransactionManager txn_manager_(&buffer_pool_, true, nullptr);
  terrier::transaction::TransactionContext *txn_ = txn_manager_.BeginTransaction();
  // create the (system) catalogs
  terrier::catalog::terrier_catalog = std::make_shared<terrier::catalog::Catalog>(&txn_manager_, txn_);
  terrier::settings::SettingsManager settings_manager_(terrier::catalog::terrier_catalog, &txn_manager_);
  LOG_INFO("Initialization complete");

  terrier::network::TerrierServer terrier_server;
  terrier_server.SetPort(static_cast<int16_t>(settings_manager_.GetInt(terrier::settings::Param::port)));
  terrier_server.SetupServer().ServerLoop();

  // TODO(pakhtar): fix so the catalog works nicely with the GC, and shutdown is clean and leak-free. (#323)
  // shutdown loggers
  spdlog::shutdown();
  main_stat_reg->Shutdown(false);
  return 0;
}

void MainDatabase::EmptyCallback(void *old_value, void *new_value) {}

void MainDatabase::BufferPoolSizeCallback(void *old_value, void *new_value) {
  int new_size = *static_cast<int *>(new_value);
  txn_manager_->SetBufferPoolSizeLimit(new_size);
}

transaction::TransactionManager *MainDatabase::txn_manager_;

}  // namespace terrier
