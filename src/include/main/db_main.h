#pragma once

#include <network/terrier_server.h>
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
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier {
class DBMain {
 public:
  int Init();
  int Start();
  void Shutdown();

 private:
  // friend class SettingsManager
  std::shared_ptr<terrier::common::StatisticsRegistry> main_stat_reg_;
};
}  // namespace terrier
