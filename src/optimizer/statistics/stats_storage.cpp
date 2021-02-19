#include "optimizer/statistics/stats_storage.h"

#include <memory>
#include <utility>

#include "loggers/optimizer_logger.h"

namespace noisepage::optimizer {

common::ManagedPointer<TableStats> StatsStorage::GetTableStats(const catalog::db_oid_t database_id,
                                                               const catalog::table_oid_t table_id,
                                                               catalog::CatalogAccessor *accessor) {
  StatsStorageKey stats_storage_key = std::make_pair(database_id, table_id);
  auto table_it = table_stats_storage_.find(stats_storage_key);

  if (table_it != table_stats_storage_.end()) {
    auto table_stat = common::ManagedPointer(table_it->second);

    // Update any stale columns
    for (auto column_stat : table_stat->GetColumnStats()) {
      if (column_stat->IsStale()) {
        auto col_oid = column_stat->GetColumnID();
        table_stat->RemoveColumnStats(col_oid);
        auto new_column_stat = accessor->GetColumnStatistics(table_id, col_oid);
        table_stat->AddColumnStats(std::move(new_column_stat));
      }
    }

    return table_stat;
  }

  InsertTableStats(database_id, table_id, accessor->GetTableStatistics(table_id));

  return common::ManagedPointer<TableStats>(table_stats_storage_.at(stats_storage_key));
}

void StatsStorage::MarkStatsStale(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                  const std::vector<catalog::col_oid_t> &col_ids) {
  StatsStorageKey stats_storage_key = std::make_pair(database_id, table_id);
  NOISEPAGE_ASSERT(table_stats_storage_.count(stats_storage_key) != 0,
                   "There is no TableStats object with the given oids");
  for (const auto &col_id : col_ids) {
    table_stats_storage_.at(stats_storage_key)->GetColumnStats(col_id)->MarkStale();
  }
}

void StatsStorage::InsertTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                    std::unique_ptr<TableStats> table_stats) {
  StatsStorageKey stats_storage_key = std::make_pair(database_id, table_id);
  NOISEPAGE_ASSERT(table_stats_storage_.count(stats_storage_key) == 0,
                   "There already exists a TableStats object with the given oids.");
  table_stats_storage_.emplace(stats_storage_key, std::move(table_stats));
}

void StatsStorage::DeleteTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id) {
  StatsStorageKey stats_storage_key = std::make_pair(database_id, table_id);
  NOISEPAGE_ASSERT(table_stats_storage_.count(stats_storage_key) != 0,
                   "There is no TableStats object with the given oids");
  table_stats_storage_.erase(table_stats_storage_.find(stats_storage_key));
}
}  // namespace noisepage::optimizer
