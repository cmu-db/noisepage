#include "optimizer/statistics/stats_storage.h"

#include <memory>
#include <utility>

#include "loggers/optimizer_logger.h"

namespace noisepage::optimizer {

StatsStorage::StatsStorage(catalog::CatalogAccessor *accessor) : accessor_(accessor) {}

// TODO(Joe) deal with dirty stats
common::ManagedPointer<TableStats> StatsStorage::GetTableStats(const catalog::db_oid_t database_id,
                                                               const catalog::table_oid_t table_id) {
  StatsStorageKey stats_storage_key = std::make_pair(database_id, table_id);
  auto table_it = table_stats_storage_.find(stats_storage_key);

  if (table_it != table_stats_storage_.end()) {
    return common::ManagedPointer<TableStats>(table_it->second);
  }

  InsertTableStats(database_id, table_id, accessor_->GetTableStatistics(table_id));

  return common::ManagedPointer<TableStats>(table_stats_storage_.at({database_id, table_id}));
}

bool StatsStorage::InsertTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                    std::unique_ptr<TableStats> table_stats) {
  StatsStorageKey stats_storage_key = std::make_pair(database_id, table_id);
  auto table_it = table_stats_storage_.find(stats_storage_key);

  if (table_it != table_stats_storage_.end()) {
    OPTIMIZER_LOG_TRACE("There already exists a TableStats object with the given oids.");
    return false;
  }
  std::unique_ptr<TableStats> table_stats_ptr = std::make_unique<TableStats>(std::move(table_stats));
  table_stats_storage_.emplace(stats_storage_key, std::move(table_stats_ptr));
  return true;
}

bool StatsStorage::DeleteTableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id) {
  StatsStorageKey stats_storage_key = std::make_pair(database_id, table_id);
  auto table_it = table_stats_storage_.find(stats_storage_key);

  if (table_it != table_stats_storage_.end()) {
    table_stats_storage_.erase(table_it);
    return true;
  }
  return false;
}
}  // namespace noisepage::optimizer
