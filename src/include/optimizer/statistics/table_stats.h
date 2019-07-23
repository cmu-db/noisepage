#pragma once

#include <sstream>

#include "catalog/catalog_defs.h"
#include "common/macros.h"
#include "optimizer/statistics/column_stats.h"
#include "storage/index/index_builder.h"

namespace terrier::optimizer {

class TableStats {
 public:
  TableStats(catalog::namespace_oid_t namespace_id, catalog::db_oid_t database_id, catalog::table_oid_t table_id,
      size_t num_rows, bool is_base_table, const std::vector<ColumnStats*> &col_stats_list)
      : namespace_id_(namespace_id),
        database_id_(database_id),
        table_id_(table_id),
        num_rows_(num_rows),
        is_base_table_(is_base_table) {
    for (auto& x : col_stats_list) {
      col_to_stats_ptr_map_.insert({x->ColumnStats::GetColumnID(), x});
    }
  }

  void UpdateNumRows(size_t new_num_rows);

  bool AddColumnStats(ColumnStats *col_stats);

  void ClearColumnStats();

  double GetCardinality(catalog::col_oid_t column_id);

  size_t GetColumnCount();

  bool HasColumnStats(catalog::col_oid_t column_id);

  ColumnStats *GetColumnStats(catalog::col_oid_t column_id);

  bool RemoveColumnStats(catalog::col_oid_t column_id);

  inline bool IsBaseTable() { return is_base_table_; }

  inline size_t& GetNumRows() { return this->num_rows_; }

  inline std::unordered_map<catalog::col_oid_t, ColumnStats*>& GetColToStatsPtrMap() {
    return this->col_to_stats_ptr_map_;
  }

  TableStats() = default;

  virtual ~TableStats() = default;

  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["namespace_id"] = namespace_id_;
    j["database_id"] = database_id_;
    j["table_id"] = table_id_;
    j["num_rows"] = num_rows_;
    j["is_base_table"] = is_base_table_;
    j["col_to_stats_ptr_map"] = col_to_stats_ptr_map_;
    return j;
  }

  void FromJson(const nlohmann::json &j) {
    namespace_id_ = j.at("namespace_id").get<catalog::namespace_oid_t>();
    database_id_ = j.at("database_id").get<catalog::db_oid_t>();
    table_id_ = j.at("table_id").get<catalog::table_oid_t>();
    num_rows_ = j.at("num_rows").get<double>();
    is_base_table_ = j.at("is_base_table").get<bool>();
    col_to_stats_ptr_map_ = j.at("col_to_stats_ptr_map").get<std::unordered_map<catalog::col_oid_t, ColumnStats*>>();
  }

  private:
    catalog::namespace_oid_t namespace_id_;
    catalog::db_oid_t database_id_;
    catalog::table_oid_t table_id_;
    size_t num_rows_;
    bool is_base_table_;
    std::unordered_map<catalog::col_oid_t, ColumnStats*> col_to_stats_ptr_map_;
};
  DEFINE_JSON_DECLARATIONS(TableStats)
}