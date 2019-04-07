#pragma once

#include <gflags/gflags.h>
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
#include "settings/settings_manager.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace  terrier{

class MainDatabase {
 public:
  static int start(int argc, char *argv[]);

  static void EmptyCallback(void *old_value UNUSED_ATTRIBUTE,
                            void *new_value UNUSED_ATTRIBUTE);
};

}
