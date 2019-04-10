#pragma once

#include <fstream>
#include <iostream>
#include <memory>
#include <vector>
#include "bwtree/bwtree.h"
#include "common/allocator.h"
#include "common/stat_registry.h"
#include "common/strong_typedef.h"
#include "loggers/index_logger.h"
#include "loggers/main_logger.h"
#include "loggers/network_logger.h"
#include "loggers/parser_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"
#include "loggers/type_logger.h"
#include "network/terrier_server.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier {

/**
 * A DBMain object holds every thing (i.e. pointers to every component)
 * about the database system.
 * *Only the settings manager should be able to access the DBMain object.*
 */
class DBMain {
 public:
  /**
   * This function initializes the following components in order:
   *    SettingsManager
   *    Garbage Collector
   *    Catalog
   *    Worker Pool
   *    Logging
   *    Stats
   * @return 0 if initialized successfully; 1 otherwise
   */
  int Init(int argc, char **argv);

  /**
   * This function boots traffic cop and networking layer
   * @return 0 if started successfully; 1 otherwise
   */
  int Start();

  /**
   * This function provides the shutdown API
   */
  void Shutdown();

 private:
  // friend class SettingsManager
  std::shared_ptr<common::StatisticsRegistry> main_stat_reg_;
  network::TerrierServer terrier_server_;
};
}  // namespace terrier
