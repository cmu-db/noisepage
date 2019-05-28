#include <gflags/gflags.h>
#include <memory>
#include <unordered_map>
#include <utility>
#include "main/db_main.h"
#include "settings/settings_manager.h"

/*
 * Define gflags configurations.
 * This will expand to a list of code like:
 * DEFINE_int32(port, 15721, "Terrier port (default: 15721)");
 */
#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

int main(int argc, char *argv[]) {
#include <vector>
#include "bwtree/bwtree.h"
#include "catalog/catalog.h"
#include "common/allocator.h"
#include "common/settings.h"
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
#include "network/connection_handle_factory.h"
#include "network/terrier_server.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "traffic_cop/traffic_cop.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
int main() {
  // initialize loggers
  // Parse Setting Values
  ::google::SetUsageMessage("Usage Info: \n");
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  std::unordered_map<terrier::settings::Param, terrier::settings::ParamInfo> param_map;

  // initialize stat registry
  auto main_stat_reg = std::make_shared<terrier::common::StatisticsRegistry>();

  terrier::settings::SettingsManager::ConstructParamMap(param_map);
  terrier::DBMain db(std::move(param_map));
  db.Init();
  db.Run();
  // create the global transaction mgr
  terrier::storage::RecordBufferSegmentPool buffer_pool_(100000, 10000);
  terrier::transaction::TransactionManager txn_manager_(&buffer_pool_, true, nullptr);
  terrier::transaction::TransactionContext *txn_ = txn_manager_.BeginTransaction();
  // create the (system) catalogs
  terrier::catalog::terrier_catalog = std::make_shared<terrier::catalog::Catalog>(&txn_manager_, txn_);
  LOG_INFO("Initialization complete");

  terrier::traffic_cop::TrafficCop t_cop;
  terrier::network::CommandFactory command_factory;

  terrier::network::ConnectionHandleFactory connection_handle_factory(&t_cop, &command_factory);
  terrier::network::TerrierServer terrier_server(&connection_handle_factory);

  terrier_server.SetPort(terrier::common::Settings::SERVER_PORT);
  terrier_server.SetupServer().ServerLoop();

  // TODO(pakhtar): fix so the catalog works nicely with the GC, and shutdown is clean and leak-free. (#323)
  // shutdown loggers
  spdlog::shutdown();
  main_stat_reg->Shutdown(false);
}
