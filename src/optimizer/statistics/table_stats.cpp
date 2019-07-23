#include "optimizer/statistics/table_stats.h"
#include "optimizer/statistics/column_stats.h"

namespace terrier::optimizer {

void TableStats::UpdateNumRows(size_t new_num_rows) {
  GetNumRows() = new_num_rows;
  for (auto& col_to_stats_pair : GetColToStatsPtrMap()) {
    auto& col_stats_ptr = col_to_stats_pair.second;
    col_stats_ptr->GetNumRows() = new_num_rows;
  }
}

bool TableStats::AddColumnStats(ColumnStats *col_stats) {
  auto it = GetColToStatsPtrMap().find(col_stats->GetColumnID());
  if (it != GetColToStatsPtrMap().end()) {
    return false;
  }
  GetColToStatsPtrMap().insert({col_stats->GetColumnID(), col_stats});
  return true;
}

void TableStats::ClearColumnStats() { GetColToStatsPtrMap().clear(); }

size_t TableStats::GetColumnCount() { return GetColToStatsPtrMap().size(); }

double TableStats::GetCardinality(catalog::col_oid_t column_id) {
  auto column_stats = GetColumnStats(column_id);
  if (column_stats == nullptr) {
    return 0;
  }
  return column_stats->GetCardinality();
}

bool TableStats::HasColumnStats(catalog::col_oid_t column_id) {
  auto col_it = GetColToStatsPtrMap().find(column_id);

  return (col_it != GetColToStatsPtrMap().end());
}

ColumnStats *TableStats::GetColumnStats(catalog::col_oid_t column_id) {
  auto col_it = GetColToStatsPtrMap().find(column_id);

  if (col_it != GetColToStatsPtrMap().end()) {
    return col_it->second;
  } else {
    return nullptr;
  }
}

bool TableStats::RemoveColumnStats(catalog::col_oid_t column_id) {
  auto col_it = GetColToStatsPtrMap().find(column_id);

  if (col_it != GetColToStatsPtrMap().end()) {
    GetColToStatsPtrMap().erase(col_it);
    return true;
  } else {
    return false;
  }
}

}
