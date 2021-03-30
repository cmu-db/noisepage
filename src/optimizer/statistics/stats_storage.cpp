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
LatchedTableStatsReference StatsStorage::GetTableStats(const catalog::db_oid_t database_id,
                                                       const catalog::table_oid_t table_id,
                                                       catalog::CatalogAccessor *accessor) {
  if (!ContainsTableStats(database_id, table_id)) {
    InsertTableStats(database_id, table_id, accessor);
  }

  common::SharedLatchGuard shared_stats_storage_latch{&stats_storage_latch_};

  TableStatsKey table_stats_key{database_id, table_id};
  auto table_it = table_stats_storage_.find(table_stats_key);

  auto &table_stats_value = table_it->second;

  UpdateStaleColumns(table_id, &table_stats_value, accessor);

  common::SharedLatchGuard shared_table_stats_latch{&table_stats_value.shared_latch_};
  return LatchedTableStatsReference(table_stats_value.table_stats_, std::move(shared_stats_storage_latch),
                                    std::move(shared_table_stats_latch));
}

void StatsStorage::MarkStatsStale(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                  const std::vector<catalog::col_oid_t> &col_ids) {
  TableStatsKey table_stats_key{database_id, table_id};
  common::SharedLatch::ScopedSharedLatch shared_stats_storage_latch{&stats_storage_latch_};
  auto table_stats_value_it = table_stats_storage_.find(table_stats_key);
  if (table_stats_value_it != table_stats_storage_.end()) {
    auto &stats_storage_value = table_stats_storage_.at(table_stats_key);
    /*
     * We don't need an exclusive latch because it's ok to mark something stale while someone is reading it. The worst
     * that happens is they end up using slightly stale statistics without realizing it.
     */
    common::SharedLatch::ScopedSharedLatch shared_table_stats_latch{&stats_storage_value.shared_latch_};
    for (const auto &col_id : col_ids) {
      stats_storage_value.table_stats_.GetColumnStats(col_id)->MarkStale();
    }
  }
}

bool StatsStorage::ContainsTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id) {
  common::SharedLatch::ScopedSharedLatch shared_stats_storage_latch{&stats_storage_latch_};
  TableStatsKey table_stats_key{database_id, table_id};
  return table_stats_storage_.count(table_stats_key) > 0;
}

void StatsStorage::InsertTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                    catalog::CatalogAccessor *accessor) {
  common::SharedLatch::ScopedExclusiveLatch exclusive_stats_storage_latch{&stats_storage_latch_};

  TableStatsKey table_stats_key{database_id, table_id};
  if (table_stats_storage_.count(table_stats_key) == 0) {
    table_stats_storage_.emplace(table_stats_key, accessor->GetTableStatistics(table_id));
  }
}

void StatsStorage::UpdateStaleColumns(catalog::table_oid_t table_id, TableStatsValue *table_stats_value,
                                      catalog::CatalogAccessor *accessor) {
  {
    common::SharedLatch::ScopedSharedLatch shared_table_latch{&table_stats_value->shared_latch_};
    if (!table_stats_value->table_stats_.HasStaleValues()) {
      return;
    }
  }

  common::SharedLatch::ScopedExclusiveLatch exclusive_table_latch{&table_stats_value->shared_latch_};

  auto &table_stats = table_stats_value->table_stats_;
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
