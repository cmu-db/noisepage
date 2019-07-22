#pragma once

#include "catalog/catalog_defs.h"
#include "common/macros.h"

#include "optimizer/statistics/table_stats.h"
#include "optimizer/statistics/column_stats.h"

#include <sstream>

namespace terrier::optimizer {

class StatsStorage {
 public:

  ColumnStats GetColumnStats(catalog::namespace_oid_t namespace_id,
                             catalog::db_oid_t database_id,
                             catalog::table_oid_t table_id,
                             catalog::col_oid_t column_id);

  TableStats GetTableStats(catalog::namespace_oid_t namespace_id,
                           catalog::db_oid_t database_id,
                           catalog::table_oid_t table_id);

  std::unordered_map<std::tuple<catalog::namespace_oid_t,
                                catalog::db_oid_t,
                                catalog::table_oid_t,
                                catalog::col_oid_t>, ColumnStats> *GetPtrToColumnStatsStorage()
                                { return &column_stats_storage; }

  std::unordered_map<std::tuple<catalog::namespace_oid_t,
                                catalog::db_oid_t,
                                catalog::table_oid_t>, TableStats> *GetPtrToTableStatsStorage()
                                { return &table_stats_storage; }

 protected:
  void InsertOrUpdateColumnStats(catalog::namespace_oid_t namespace_id,
                                 catalog::db_oid_t database_id,
                                 catalog::table_oid_t table_id,
                                 catalog::col_oid_t column_id,
                                 const ColumnStats &column_stats);

  void DeleteColumnStats(catalog::namespace_oid_t namespace_id,
                         catalog::db_oid_t database_id,
                         catalog::table_oid_t table_id,
                         catalog::col_oid_t column_id);

  void InsertOrUpdateTableStats(catalog::namespace_oid_t namespace_id,
                                catalog::db_oid_t database_id,
                                catalog::table_oid_t table_id,
                                const TableStats &table_stats);

  void DeleteTableStats(catalog::namespace_oid_t namespace_id,
                        catalog::db_oid_t database_id,
                        catalog::table_oid_t table_id);

 private:
  std::unordered_map<std::tuple<catalog::namespace_oid_t,
                                catalog::db_oid_t,
                                catalog::table_oid_t,
                                catalog::col_oid_t>, ColumnStats> column_stats_storage;

  std::unordered_map<std::tuple<catalog::namespace_oid_t,
                                catalog::db_oid_t,
                                catalog::table_oid_t>, TableStats> table_stats_storage;
};
}