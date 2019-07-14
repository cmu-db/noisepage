#pragma once

#include <vector>

#include "optimizer/stats/column_stats_collector.h"
#include "catalog/schema.h"

namespace terrier::optimizer {
//===--------------------------------------------------------------------===//
// TableStatsCollector
//===--------------------------------------------------------------------===//
class TableStatsCollector {
 public:
  TableStatsCollector(storage::DataTable* table);

  ~TableStatsCollector();

  void CollectColumnStats();

  inline size_t GetActiveTupleCount() { return active_tuple_count_; }

  inline size_t GetColumnCount() { return column_count_; }

  ColumnStatsCollector* GetColumnStats(oid_t column_id);

 private:
  std::vector<std::unique_ptr<ColumnStatsCollector>> column_stats_collectors_;
  size_t active_tuple_count_;
  size_t column_count_;

  TableStatsCollector(const TableStatsCollector&);
  void operator=(const TableStatsCollector&);

  void InitColumnStatsCollectors();
};

}