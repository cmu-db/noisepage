#include "stats/database_metric.h"
#include <stats/thread_level_stats_collector.h>
#include <util/transaction_benchmark_util.h>
#include "catalog/catalog_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::stats {

void DatabaseMetricRawData::UpdateAndPersist() {
  auto txn_manager = ThreadLevelStatsCollector::GetTxnManager();
  auto txn = txn_manager->BeginTransaction();

  // TODO(Wen) find a way to store collected data


  // TODO(Wen) might need to change this line
  txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
}

}  // namespace terrier::stats
