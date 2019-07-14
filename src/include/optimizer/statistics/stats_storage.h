#pragma once

#include "catalog/catalog_defs.h"

#include "optimizer/statistics/table_stats.h"
#include "optimizer/statistics/column_stats.h"

#include <sstream>

#include "common/macros.h"
#include "type/transient_value_factory.h"

namespace terrier::optimizer {

class ColumnStats;
class TableStats;

class StatsStorage {
 public:

  std::unordered_map<catalog::db_oid_t, std::unordered_map<catalog::table_oid_t,
  std::unordered_map<catalog::col_oid_t, ColumnStats>>> stats_storage;

  ColumnStats GetColumnStatsByID(catalog::db_oid_t database_id,
                                 catalog::table_oid_t table_id,
                                 catalog::col_oid_t column_id);

  std::unordered_map<catalog::col_oid_t, ColumnStats> GetTableStats(catalog::db_oid_t database_id,
                                                                    catalog::table_oid_t table_id);

};
}