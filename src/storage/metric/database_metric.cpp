#include "storage/metric/database_metric.h"
#include "catalog/catalog_defs.h"
#include "storage/metric/thread_level_stats_collector.h"
#include "transaction/transaction_manager.h"
#include "util/transaction_benchmark_util.h"

namespace terrier::storage::metric {

void DatabaseMetricRawData::UpdateAndPersist(transaction::TransactionManager *const txn_manager) {
  auto txn = txn_manager->BeginTransaction();

  // TODO(Wen) find a way to store collected data

  // TODO(Wen) might need to change this line
  txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
}

}  // namespace terrier::storage::metric
