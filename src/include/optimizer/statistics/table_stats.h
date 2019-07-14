#pragma once

#include <sstream>

#include "catalog/catalog_defs.h"
#include "common/macros.h"

namespace terrier::optimizer {

#define DEFAULT_CARDINALITY 1
#define DEFAULT_HAS_INDEX false

class ColumnStats;

class TableStats {
 public:
  TableStats() : TableStats((size_t)0) {}

  TableStats(size_t num_rows, bool is_base_table = true)
      : Stats(nullptr),
        num_rows(num_rows),
        col_stats_list_{},
        col_name_to_stats_map_{},
        is_base_table_(is_base_table),

  TableStats(size_t num_rows,
             std::vector<std::shared_ptr<ColumnStats>> col_stats_ptrs,
             bool is_base_table = true);

  TableStats(std::vector<std::shared_ptr<ColumnStats>> col_stats_ptrs,
             bool is_base_table = true);

  /*
   * Right now table_stats need to support both column id and column name
   * lookup to support both base and intermediate table with alias.
   */

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

  std::shared_ptr<ColumnStats> GetColumnStats(const std::string col_name);

  std::shared_ptr<ColumnStats> GetColumnStats(const catalog::col_oid_t column_id);

  bool AddColumnStats(std::shared_ptr<ColumnStats> col_stats);

  bool RemoveColumnStats(const std::string col_name);

  bool RemoveColumnStats(const catalog::col_oid_t column_id);

  bool AddIndex(std::string key, const std::shared_ptr<index::Index> index);

  std::shared_ptr<index::Index> GetIndex(const std::string col_name);

  inline bool IsBaseTable() { return is_base_table_; }

  void UpdateJoinColumnStats(std::vector<catalog::col_oid_t> &column_ids);

  size_t GetColumnCount();

  size_t num_rows;

 private:
  // TODO: only keep one ptr of ColumnStats
  std::vector<std::shared_ptr<ColumnStats>> col_stats_list_;
  std::unordered_map<std::string, std::shared_ptr<ColumnStats>>
      col_name_to_stats_map_;
  std::unordered_map<std::string, std::shared_ptr<index::Index>> index_map_;
  bool is_base_table_;

};

}