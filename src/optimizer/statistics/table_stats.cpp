#include "optimizer/statistics/table_stats.h"
#include "optimizer/statistics/column_stats.h"

namespace terrier::optimizer {

void TableStats::UpdateNumRows(size_t new_num_rows) {
  *TableStats::GetPtrToNumRows() = new_num_rows;
  for (auto& col_name_stats_pair : col_name_to_stats_map_) {
    auto& col_stats = col_name_stats_pair.second;
    col_stats->num_rows = num_rows;
  }
}

bool AddColumnStats(std::shared_ptr<ColumnStats> col_stats) {
  auto it = col_name_to_stats_map_.find(col_stats->column_name);
  if (it != col_name_to_stats_map_.end()) {
    return false;
  }
  col_name_to_stats_map_.insert({col_stats->column_name, col_stats});
  return true;
}

void ClearColumnStats() { col_name_to_stats_map_.clear(); }

size_t GetColumnCount() { return col_stats_list_.size(); }

bool AddIndex(std::string key, std::shared_ptr<index::Index> index_) {
  // Only consider adding single column index for now
  if (index_->GetColumnCount() > 1) return false;

  if (index_map_.find(key) == index_map_.end()) {
    index_map_.insert({key, index_});
    return true;
  }
  return false;
}


//===--------------------------------------------------------------------===//
// TableStats with column_name operations
//===--------------------------------------------------------------------===//
bool TableStats::HasIndex(const std::string column_name) {
  auto column_stats = GetColumnStats(column_name);
  if (column_stats == nullptr) {
    return DEFAULT_HAS_INDEX;
  }
  return column_stats->has_index;
}

bool TableStats::HasPrimaryIndex(const std::string column_name) {
  return HasIndex(column_name);
}

double TableStats::GetCardinality(const std::string column_name) {
  auto column_stats = GetColumnStats(column_name);
  if (column_stats == nullptr) {
    return DEFAULT_CARDINALITY;
  }
  return column_stats->cardinality;
}

bool TableStats::HasColumnStats(const std::string col_name) {
  auto it = col_name_to_stats_map_.find(col_name);
  if (it == col_name_to_stats_map_.end()) {
    return false;
  }
  return true;
}

std::shared_ptr<ColumnStats> TableStats::GetColumnStats(
    const std::string col_name) {
  auto it = col_name_to_stats_map_.find(col_name);
  if (it != col_name_to_stats_map_.end()) {
    return it->second;
  }
  return nullptr;
}

std::shared_ptr<index::Index> TableStats::GetIndex(std::string col_name) {
  if (index_map_.find(col_name) != index_map_.end()) {
    return index_map_.find(col_name)->second;
  }
  return std::shared_ptr<index::Index>(nullptr);
}

bool TableStats::RemoveColumnStats(const std::string col_name) {
  auto it = col_name_to_stats_map_.find(col_name);
  if (it == col_name_to_stats_map_.end()) {
    return false;
  }
  col_name_to_stats_map_.erase(col_name);
  return true;
}

//===--------------------------------------------------------------------===//
// TableStats with column_id operations
//===--------------------------------------------------------------------===//
bool TableStats::HasIndex(const oid_t column_id) {
  auto column_stats = GetColumnStats(column_id);
  if (column_stats == nullptr) {
    return false;
  }
  return column_stats->has_index;
}

// Update this function once we support primary index operations.
bool TableStats::HasPrimaryIndex(const oid_t column_id) {
  return HasIndex(column_id);
}

double TableStats::GetCardinality(const oid_t column_id) {
  auto column_stats = GetColumnStats(column_id);
  if (column_stats == nullptr) {
    return DEFAULT_CARDINALITY;
  }
  return column_stats->cardinality;
}

bool TableStats::HasColumnStats(const oid_t column_id) {
  return column_id < col_stats_list_.size();
}

std::shared_ptr<ColumnStats> TableStats::GetColumnStats(const oid_t column_id) {
  if (column_id >= col_stats_list_.size()) {
    return nullptr;
  }
  return col_stats_list_[column_id];
}

bool TableStats::RemoveColumnStats(const oid_t column_id) {
  if (column_id >= col_stats_list_.size()) {
    return false;
  }
  col_stats_list_.erase(col_stats_list_.begin() + column_id);
  return true;
}

}
