#include "loggers/optimizer_logger.h"

#include "optimizer/statistics/stats_storage.h"

namespace terrier::optimizer {

TableStats *StatsStorage::GetPtrToTableStats(catalog::db_oid_t database_id,
                                             catalog::table_oid_t table_id) {
  auto stats_storage_key = StatsStorageKey(database_id, table_id);
  auto table_it = (*StatsStorage::GetPtrToTableStatsStorage()).find(stats_storage_key);

  if (table_it != (*StatsStorage::GetPtrToTableStatsStorage()).end()) {
    return table_it->second;
  } else {
    OPTIMIZER_LOG_TRACE("One or more ids given don't exist in table stats storage map.")
  }
}

void StatsStorage::InsertOrUpdateTableStats(catalog::db_oid_t database_id,
                                            catalog::table_oid_t table_id, TableStats *table_stats) {
  auto stats_storage_key = StatsStorageKey(database_id, table_id);
  auto table_it = (*StatsStorage::GetPtrToTableStatsStorage()).find(stats_storage_key);

  if (table_it != (*StatsStorage::GetPtrToTableStatsStorage()).end()) {
    (*StatsStorage::GetPtrToTableStatsStorage()).erase(table_it);
  }
  (*StatsStorage::GetPtrToTableStatsStorage()).insert({stats_storage_key, table_stats});
}

void StatsStorage::DeleteTableStats(catalog::db_oid_t database_id,
                                    catalog::table_oid_t table_id) {
  auto stats_storage_key = StatsStorageKey(database_id, table_id);
  auto table_it = (*StatsStorage::GetPtrToTableStatsStorage()).find(stats_storage_key);

  if (table_it != (*StatsStorage::GetPtrToTableStatsStorage()).end()) {
    (*StatsStorage::GetPtrToTableStatsStorage()).erase(table_it);
  }
}

}  // namespace terrier::optimizer
