#include "loggers/optimizer_logger.h"

#include "optimizer/statistics/stats_storage.h"

namespace terrier::optimizer {

common::ManagedPointer<TableStats> StatsStorage::GetPtrToTableStats(catalog::db_oid_t database_id,
                                                                    catalog::table_oid_t table_id) {
  auto stats_storage_key = StatsStorageKey(database_id, table_id);
  auto table_it = StatsStorage::GetPtrToTableStatsStorage()->find(stats_storage_key);

  if (table_it != StatsStorage::GetPtrToTableStatsStorage()->end()) {
    return common::ManagedPointer<TableStats>(table_it->second);
  } else {
    return common::ManagedPointer<TableStats>(nullptr);
  }
}

bool StatsStorage::InsertTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                    std::unique_ptr<TableStats> table_stats) {
  auto stats_storage_key = StatsStorageKey(database_id, table_id);
  auto table_it = StatsStorage::GetPtrToTableStatsStorage()->find(stats_storage_key);

  if (table_it != StatsStorage::GetPtrToTableStatsStorage()->end()) {
    return false;
  }
  StatsStorage::GetPtrToTableStatsStorage()->insert({stats_storage_key, std::move(table_stats)});
  return true;
}

bool StatsStorage::DeleteTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id) {
  auto stats_storage_key = StatsStorageKey(database_id, table_id);
  auto table_it = StatsStorage::GetPtrToTableStatsStorage()->find(stats_storage_key);

  if (table_it != StatsStorage::GetPtrToTableStatsStorage()->end()) {
    StatsStorage::GetPtrToTableStatsStorage()->erase(table_it);
    return true;
  }
  return false;
}

}  // namespace terrier::optimizer
