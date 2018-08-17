#include <iostream>
#include "common/main_stat_registry.h"
#include "common/typedefs.h"
#include "loggers/main_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"
#include "storage/data_table.h"
#include "storage/storage_defs.h"

int main() {
  // initialize loggers
  try {
    init_main_logger();
    // initialize namespace specific loggers
    terrier::storage::init_storage_logger();
    terrier::transaction::init_transaction_logger();

    // Flush all *registered* loggers using a worker thread.
    // Registered loggers must be thread safe for this to work correctly
    spdlog::flush_every(std::chrono::seconds(DEBUG_LOG_FLUSH_INTERVAL));
  }
  catch (const spdlog::spdlog_ex &ex) {
    std::cout << "debug log init failed " << ex.what() << std::endl;
    return 1;
  }

  // log init now complete
  LOG_INFO("woof!");
  std::cout << "hello world!" << std::endl;

  // initialize stat registry
  init_main_stat_reg();
  terrier::common::ObjectPool<terrier::transaction::UndoBufferSegment> buffer_pool_{10000};
  terrier::storage::BlockStore block_store_{100};
  terrier::storage::BlockLayout block_layout_(2, {8, 16});
  terrier::storage::DataTable data_table_(&block_store_, block_layout_);

  terrier::timestamp_t timestamp(0);
  auto *txn = new terrier::transaction::TransactionContext(timestamp, timestamp, &buffer_pool_);

  auto *redo_buffer = new terrier::byte[1000000];
  terrier::storage::ProjectedRow *redo =
      terrier::storage::ProjectedRow::InitializeProjectedRow(redo_buffer, {0, 1}, block_layout_);

  data_table_.Insert(txn, *redo);

  std::cout << STAT_DUMP_STATS({}, 4);
  std::cout << std::endl;

  // shutdown loggers
  spdlog::shutdown();
}
