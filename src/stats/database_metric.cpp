#include "stats/database_metric.h"
#include "catalog/catalog_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::metric {

void DatabaseMetricRawData::UpdateAndPersist() {
  auto &txn_manager = transaction::;
  auto txn = txn_manager.BeginTransaction();
  auto time_since_epoch = std::chrono::system_clock::now().time_since_epoch();
  auto time_stamp = std::chrono::duration_cast<std::chrono::seconds>(time_since_epoch).count();

  auto database_metrics_catalog = catalog::DatabaseMetricsCatalog::GetInstance();
  for (auto &entry : counters_) {
    // one iteration per database
    catalog::db_oid_t database_oid = entry.first;
    auto &counts = entry.second;

    auto old_metric = database_metrics_catalog->GetDatabaseMetricsObject(database_oid, txn);
    if (old_metric == nullptr) {
      // no entry exists for this database yet
      database_metrics_catalog->InsertDatabaseMetrics(database_oid, counts.first, counts.second, time_stamp, nullptr,
                                                      txn);
    } else {
      // update existing entry
      database_metrics_catalog->UpdateDatabaseMetrics(database_oid, old_metric->GetTxnCommitted() + counts.first,
                                                      old_metric->GetTxnAborted() + counts.second, time_stamp, txn);
    }
  }

  txn_manager.CommitTransaction(txn);
}

}  // namespace terrier::metric
