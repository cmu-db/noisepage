#include "loggers/optimizer_logger.h"

#include "optimizer/statistics/stats_storage.h"

namespace terrier::optimizer {

/**
 * GetColumnStats - Using given namespace, database, table, and column ids,
 * select a ColumnStats object in the column stats storage map
 */

ColumnStats StatsStorage::GetColumnStats(catalog::namespace_oid_t namespace_id,
                           catalog::db_oid_t database_id,
                           catalog::table_oid_t table_id,
                           catalog::col_oid_t column_id) {
  auto column_it = (*StatsStorage::GetPtrToColumnStatsStorage()).find(std::make_tuple(namespace_id, database_id,
                                                                                      table_id, column_id));

  if (column_it != (*StatsStorage::GetPtrToColumnStatsStorage()).end()) {
    return column_it->second;
  } else {
    OPTIMIZER_LOG_TRACE("One or more ids given don't exist in column stats storage map.")
  }
}

/**
 * GetTableStats - Using given namespace, database, and table ids,
 * select a TableStats objects in the table stats storage map
 */
TableStats StatsStorage::GetTableStats(catalog::namespace_oid_t namespace_id,
                         catalog::db_oid_t database_id,
                         catalog::table_oid_t table_id) {
  auto table_it = (*StatsStorage::GetPtrToTableStatsStorage()).find(std::make_tuple(namespace_id, database_id,
                                                                                    table_id));

  if (table_it != (*StatsStorage::GetPtrToTableStatsStorage()).end()) {
    return table_it->second;
  } else {
    OPTIMIZER_LOG_TRACE("One or more ids given don't exist in table stats storage map.")
  }
}

void StatsStorage::InsertOrUpdateColumnStats(catalog::namespace_oid_t namespace_id,
                               catalog::db_oid_t database_id,
                               catalog::table_oid_t table_id,
                               catalog::col_oid_t column_id,
                               const ColumnStats &column_stats) {
  auto column_it = (*StatsStorage::GetPtrToColumnStatsStorage()).find(std::make_tuple(namespace_id, database_id,
                                                                                      table_id, column_id));

  if (column_it != (*StatsStorage::GetPtrToColumnStatsStorage()).end()) {
    (*StatsStorage::GetPtrToColumnStatsStorage()).erase(column_it);
  }
  (*StatsStorage::GetPtrToColumnStatsStorage()).insert({ std::make_tuple(namespace_id, database_id, table_id,
                                                                         column_id), column_stats });
}

void StatsStorage::DeleteColumnStats(catalog::namespace_oid_t namespace_id,
                       catalog::db_oid_t database_id,
                       catalog::table_oid_t table_id,
                       catalog::col_oid_t column_id) {
  auto column_it = (*StatsStorage::GetPtrToColumnStatsStorage()).find(std::make_tuple(namespace_id, database_id,
                                                                                      table_id, column_id));

  if (column_it != (*StatsStorage::GetPtrToColumnStatsStorage()).end()) {
    (*StatsStorage::GetPtrToColumnStatsStorage()).erase(column_it);
  }
}

void StatsStorage::InsertOrUpdateTableStats(catalog::namespace_oid_t namespace_id,
                              catalog::db_oid_t database_id,
                              catalog::table_oid_t table_id,
                              const TableStats &table_stats) {
  auto table_it = (*StatsStorage::GetPtrToTableStatsStorage()).find(std::make_tuple(namespace_id, database_id,
                                                                                    table_id));

  if (table_it != (*StatsStorage::GetPtrToTableStatsStorage()).end()) {
    (*StatsStorage::GetPtrToTableStatsStorage()).erase(table_it);
  }
  (*StatsStorage::GetPtrToTableStatsStorage()).insert({ std::make_tuple(namespace_id, database_id, table_id),
                                                          table_stats });
}

void StatsStorage::DeleteTableStats(catalog::namespace_oid_t namespace_id,
                      catalog::db_oid_t database_id,
                      catalog::table_oid_t table_id) {
  auto table_it = (*StatsStorage::GetPtrToTableStatsStorage()).find(std::make_tuple(namespace_id, database_id,
                                                                                    table_id));

  if (table_it != (*StatsStorage::GetPtrToTableStatsStorage()).end()) {
    (*StatsStorage::GetPtrToTableStatsStorage()).erase(table_it);
  }
}

}
