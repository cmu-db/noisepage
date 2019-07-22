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
      size_t num_rows, bool is_base_table = true, std::vector<ColumnStats> col_stats_list)
      : namespace_id_(namespace_id),
        database_id_(database_id),
        table_id_(table_id),
        num_rows_(num_rows),
        is_base_table_(is_base_table),
        col_stats_list_(std::move(col_stats_list)),
        col_name_to_stats_map_{} {}

  void UpdateNumRows(size_t new_num_rows);

  bool HasIndex(const std::string column_name);

  bool HasIndex(const catalog::col_oid_t column_id);

  bool HasPrimaryIndex(const std::string column_name);

  bool HasPrimaryIndex(const catalog::col_oid_t column_id);

  double GetCardinality(const std::string column_name);

  double GetCardinality(const catalog::col_oid_t column_id);

  void ClearColumnStats();

  bool HasColumnStats(const std::string col_name);

  bool HasColumnStats(const catalog::col_oid_t column_id);

  std::vector<ColumnStats> GetColumnStats(const std::string col_name);

  std::vector<ColumnStats> GetColumnStats(const catalog::col_oid_t column_id);

  bool AddColumnStats(ColumnStats col_stats);

  bool RemoveColumnStats(const std::string col_name);

  bool RemoveColumnStats(const catalog::col_oid_t column_id);

  bool AddIndex(std::string key, const std::shared_ptr<storage::index::Index> index);

  std::shared_ptr<storage::index::Index> GetIndex(const std::string col_name);

  inline bool IsBaseTable() { return is_base_table_; }

  inline size_t *GetPtrToNumRows() { return &num_rows_; }

  size_t GetColumnCount();

  TableStats() = default;

  virtual ~ColumnStats() = default;

  nlohmann::json ToJson() const {
    nlohmann::json j;
  }

  void FromJson(const nlohmann::json &j) {

  }

 private:
  catalog::namespace_oid_t namespace_id_;
  catalog::db_oid_t database_id_;
  catalog::table_oid_t table_id_;
  size_t num_rows_;
  bool is_base_table_;
  std::vector<ColumnStats> col_stats_list_;
  std::unordered_map<std::string, ColumnStats> col_name_to_stats_map_;
  std::unordered_map<std::string, std::shared_ptr<storage::index::Index>> index_map_;

};

}