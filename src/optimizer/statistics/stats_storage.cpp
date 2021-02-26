#include "optimizer/statistics/stats_storage.h"

#include <memory>
#include <utility>

#include "loggers/optimizer_logger.h"

namespace noisepage::optimizer {

StatsStorageValue &StatsStorage::GetStatsStorageValue(const catalog::db_oid_t database_id, const catalog::table_oid_t table_id,
                                                      catalog::CatalogAccessor *accessor) {
  StatsStorageKey stats_storage_key{database_id, table_id};
  auto table_it = table_stats_storage_.find(stats_storage_key);

  if (table_it == table_stats_storage_.end()) {
    InsertTableStats(database_id, table_id, accessor);
    table_it = table_stats_storage_.find(stats_storage_key);
  }

  auto &stats_storage_value = table_it->second;

  UpdateStaleColumns(table_id, &stats_storage_value, accessor);

  return stats_storage_value;
}

/*
 * Currently when getting statistics for a table we get and cache the statistics for the entire table. If we find that
 * large tables are filling up the cache when we only need some of the columns, we may want to revisit this so we only
 * cache the columns we use.
 */
std::unique_ptr<TableStats> StatsStorage::GetTableStats(const catalog::db_oid_t database_id,
                                                        const catalog::table_oid_t table_id,
                                                        catalog::CatalogAccessor *accessor) {
  auto &stats_storage_value = GetStatsStorageValue(database_id, table_id, accessor);
  common::SharedLatch::ScopedSharedLatch table_latch{&stats_storage_value.shared_latch_};
  return stats_storage_value.table_stats_->Copy();
}

/*
 * Currently when getting the statistics for a column we get and cache the statistics for the entire table. If we find
 * that large tables are filling up the cache, we may want to revisit this so we only cache the column requested.
 */
std::unique_ptr<ColumnStatsBase> StatsStorage::GetColumnStats(catalog::db_oid_t database_id,
                                                              catalog::table_oid_t table_id,
                                                              catalog::col_oid_t column_oid,
                                                              catalog::CatalogAccessor *accessor) {
  auto &stats_storage_value = GetStatsStorageValue(database_id, table_id, accessor);
  common::SharedLatch::ScopedSharedLatch table_latch{&stats_storage_value.shared_latch_};
  NOISEPAGE_ASSERT(stats_storage_value.table_stats_->HasColumnStats(column_oid), "Should have stats for all columns");
  return stats_storage_value.table_stats_->GetColumnStats(column_oid)->Copy();
}

void StatsStorage::MarkStatsStale(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                  const std::vector<catalog::col_oid_t> &col_ids) {
  StatsStorageKey stats_storage_key{database_id, table_id};
  NOISEPAGE_ASSERT(table_stats_storage_.count(stats_storage_key) != 0,
                   "There is no TableStats object with the given oids");
  for (const auto &col_id : col_ids) {
    table_stats_storage_.at(stats_storage_key).table_stats_->GetColumnStats(col_id)->MarkStale();
  }
}

void StatsStorage::InsertTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                    catalog::CatalogAccessor *accessor) {
  std::unique_lock<std::mutex> latch(insert_latch_);
  StatsStorageKey stats_storage_key{database_id, table_id};
  if (table_stats_storage_.count(stats_storage_key) == 0) {
    auto table_stats = accessor->GetTableStatistics(table_id);
    table_stats_storage_.emplace(stats_storage_key, std::move(table_stats));
  }
}

void StatsStorage::UpdateStaleColumns(catalog::table_oid_t table_id, StatsStorageValue *stats_storage_value,
                                      catalog::CatalogAccessor *accessor) {
  {
    common::SharedLatch::ScopedSharedLatch shared_table_latch{&stats_storage_value->shared_latch_};
    if (!stats_storage_value->table_stats_->HasStaleValues()) {
      return;
    }
  }

  common::SharedLatch::ScopedExclusiveLatch exclusive_table_latch{&stats_storage_value->shared_latch_};

  auto &table_stats = stats_storage_value->table_stats_;
  for (auto column_stat : table_stats->GetColumnStats()) {
    if (column_stat->IsStale()) {
      auto col_oid = column_stat->GetColumnID();
      table_stats->RemoveColumnStats(col_oid);
      auto new_column_stat = accessor->GetColumnStatistics(table_id, col_oid);
      table_stats->AddColumnStats(std::move(new_column_stat));
    }
  }
}

}  // namespace noisepage::optimizer
