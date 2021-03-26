#include "optimizer/statistics/stats_storage.h"

#include <memory>
#include <utility>

#include "loggers/optimizer_logger.h"

namespace noisepage::optimizer {

/*
 * Currently when getting statistics for a table we get and cache the statistics for the entire table. If we find that
 * large tables are filling up the cache when we only need some of the columns, we may want to revisit this so we only
 * cache the columns we use.
 */
StatsStorageReference StatsStorage::GetTableStats(const catalog::db_oid_t database_id,
                                                  const catalog::table_oid_t table_id,
                                                  catalog::CatalogAccessor *accessor) {
  if (!ContainsTableStats(database_id, table_id)) {
    InsertTableStats(database_id, table_id, accessor);
  }

  common::SharedLatchGuard shared_stats_storage_latch{&stats_storage_latch_};

  StatsStorageKey stats_storage_key{database_id, table_id};
  auto table_it = table_stats_storage_.find(stats_storage_key);

  auto &stats_storage_value = table_it->second;

  UpdateStaleColumns(table_id, &stats_storage_value, accessor);

  return StatsStorageReference(&stats_storage_value, std::move(shared_stats_storage_latch));
}

void StatsStorage::MarkStatsStale(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                  const std::vector<catalog::col_oid_t> &col_ids) {
  /*
   * It's ok to mark something stale while someone is reading it, the worst that happens is they end up using slightly
   * stale statistics without realizing it.
   */
  StatsStorageKey stats_storage_key{database_id, table_id};
  common::SharedLatch::ScopedSharedLatch shared_stats_storage_latch{&stats_storage_latch_};
  auto stats_storage_value = table_stats_storage_.find(stats_storage_key);
  if (stats_storage_value != table_stats_storage_.end()) {
    for (const auto &col_id : col_ids) {
      table_stats_storage_.at(stats_storage_key).table_stats_.GetColumnStats(col_id)->MarkStale();
    }
  }
}

bool StatsStorage::ContainsTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id) {
  common::SharedLatch::ScopedSharedLatch shared_stats_storage_latch{&stats_storage_latch_};
  StatsStorageKey stats_storage_key{database_id, table_id};
  return table_stats_storage_.count(stats_storage_key) > 0;
}

void StatsStorage::InsertTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                    catalog::CatalogAccessor *accessor) {
  common::SharedLatch::ScopedExclusiveLatch exclusive_stats_storage_latch{&stats_storage_latch_};

  StatsStorageKey stats_storage_key{database_id, table_id};
  if (table_stats_storage_.count(stats_storage_key) == 0) {
    table_stats_storage_.emplace(stats_storage_key, accessor->GetTableStatistics(table_id));
  }
}

void StatsStorage::UpdateStaleColumns(catalog::table_oid_t table_id, StatsStorageValue *stats_storage_value,
                                      catalog::CatalogAccessor *accessor) {
  {
    common::SharedLatch::ScopedSharedLatch shared_table_latch{&stats_storage_value->shared_latch_};
    if (!stats_storage_value->table_stats_.HasStaleValues()) {
      return;
    }
  }

  common::SharedLatch::ScopedExclusiveLatch exclusive_table_latch{&stats_storage_value->shared_latch_};

  auto &table_stats = stats_storage_value->table_stats_;
  for (auto column_stat : table_stats.GetColumnStats()) {
    if (column_stat->IsStale()) {
      auto col_oid = column_stat->GetColumnID();
      table_stats.RemoveColumnStats(col_oid);
      auto new_column_stat = accessor->GetColumnStatistics(table_id, col_oid);
      if (new_column_stat->GetNumRows() != table_stats.GetNumRows()) {
        table_stats.SetNumRows(new_column_stat->GetNumRows());
      }
      table_stats.AddColumnStats(std::move(new_column_stat));
    }
  }
}

}  // namespace noisepage::optimizer
