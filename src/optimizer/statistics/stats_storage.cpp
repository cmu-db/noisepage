#include "loggers/optimizer_logger.h"

#include "optimizer/statistics/column_stats.h"
#include "optimizer/statistics/stats_storage.h"
#include "optimizer/statistics/table_stats.h"

namespace terrier::optimizer {

/**
 * GetColumnStatsByID - Using given database, table, and column ids,
 * select a ColumnStats object in the stats storage map
 */
ColumnStats GetColumnStatsByID(catalog::db_oid_t database_id,
    catalog::table_oid_t table_id,
    catalog::col_oid_t column_id) {
  auto tables_it = stats_storage.find(database_id);

  if (tables_it != stats_storage.end()) {
    auto columns_it = *tables_it.find(table_id);
    if (columns_it != tables_it.end()) {
      auto col_it = *columns_it.find(column_id);
      if (col_it != columns_it.end()) {
        return *col_it;
      } else {
        OPTIMIZER_LOG_TRACE("column_id not a key in the columns map.")
      }
    } else {
      OPTIMIZER_LOG_TRACE("table_id not a key in the tables map.")
    }
  } else {
    OPTIMIZER_LOG_TRACE("database_id not a key in the stats storage map.")
  }
}

/**
 * GetTableStats - Using given database and table ids,
 * select a map of column_stat ids to ColumnStats objects in the
 * stats storage map
 */
std::unordered_map<catalog::col_oid_t, ColumnStats> GetTableStats(catalog::db_oid_t database_id,
    catalog::table_oid_t table_id) {
  auto tables_it = stats_storage.find(database_id);

  if (tables_it != stats_storage.end()) {
    auto columns_it = *tables_it.find(table_id);
    if (columns_it != tables_it.end()) {
      return *columns_it;
    } else {
      OPTIMIZER_LOG_TRACE("table_id not a key in the tables map.")
    }
  } else {
    OPTIMIZER_LOG_TRACE("database_id not a key in the stats storage map.")
  }
}

}