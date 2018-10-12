#include <fstream>
#include <iostream>
#include <memory>
#include <vector>
#include "common/allocator.h"
#include "common/stat_registry.h"
#include "common/typedefs.h"
#include "loggers/main_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/expression_defs.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/tuple_value_expression.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

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
  } catch (const spdlog::spdlog_ex &ex) {
    std::cout << "debug log init failed " << ex.what() << std::endl;
    return 1;
  }

  // log init now complete
  LOG_INFO("woof!");
  std::cout << "hello world!" << std::endl;

  terrier::storage::RecordBufferSegmentPool buffer_pool_{100000, 10000};
  terrier::storage::BlockStore block_store_{1000, 100};
  terrier::storage::BlockLayout block_layout_({4, 8});
  const std::vector<terrier::col_id_t> col_ids = {terrier::col_id_t{0}, terrier::col_id_t{1}};
  terrier::storage::DataTable data_table_(&block_store_, block_layout_, terrier::layout_version_t{0});
  terrier::timestamp_t timestamp(0);
  auto *txn = new terrier::transaction::TransactionContext(timestamp, timestamp, &buffer_pool_, LOGGING_DISABLED);
  auto init = terrier::storage::ProjectedRowInitializer(block_layout_, col_ids);
  auto *redo_buffer_ = terrier::common::AllocationUtil::AllocateAligned(init.ProjectedRowSize());
  auto *redo = init.InitializeRow(redo_buffer_);

  data_table_.Insert(txn, *redo);

  // initialize stat registry
  auto main_stat_reg = std::make_shared<terrier::common::StatisticsRegistry>();
  main_stat_reg->Register({"Storage"}, data_table_.GetDataTableCounter(), &data_table_);
  std::cout << main_stat_reg->DumpStats() << std::endl;
  delete[] redo_buffer_;
  delete txn;
  // shutdown loggers
  spdlog::shutdown();
  main_stat_reg->Shutdown(false);
}
