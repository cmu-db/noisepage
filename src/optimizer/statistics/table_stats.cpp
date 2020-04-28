#include "optimizer/statistics/table_stats.h"

#include <cstddef>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "common/strong_typedef.h"
#include "loggers/optimizer_logger.h"
#include "optimizer/statistics/column_stats.h"

namespace terrier::optimizer {

void TableStats::UpdateNumRows(size_t new_num_rows) {
  num_rows_ = new_num_rows;
  for (auto &col_to_stats_pair : column_stats_) {
    auto &col_stats_ptr = col_to_stats_pair.second;
    col_stats_ptr->GetNumRows() = new_num_rows;
  }
}

bool TableStats::AddColumnStats(std::unique_ptr<ColumnStats> col_stats) {
  auto it = column_stats_.find(col_stats->GetColumnID());
  if (it != column_stats_.end()) {
    OPTIMIZER_LOG_TRACE("There already exists a ColumnStats object with the same oid.")
    return false;
  }
  column_stats_.insert({col_stats->GetColumnID(), std::move(col_stats)});
  return true;
}

double TableStats::GetCardinality(catalog::col_oid_t column_id) {
  if (!HasColumnStats(column_id)) {
    return 0;
  }

  return GetColumnStats(column_id)->GetCardinality();
}

bool TableStats::HasColumnStats(catalog::col_oid_t column_id) const {
  return (column_stats_.find(column_id) != column_stats_.end());
}

common::ManagedPointer<ColumnStats> TableStats::GetColumnStats(catalog::col_oid_t column_id) {
  auto col_it = column_stats_.find(column_id);
  if (col_it == column_stats_.end()) return nullptr;
  return common::ManagedPointer<ColumnStats>(col_it->second);
}

bool TableStats::RemoveColumnStats(catalog::col_oid_t column_id) {
  auto col_it = column_stats_.find(column_id);

  if (col_it != column_stats_.end()) {
    column_stats_.erase(col_it);
    return true;
  }
  return false;
}

}  // namespace terrier::optimizer
