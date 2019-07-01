#include "loggers/optimizer_logger.h"
#include "optimizer/statistics/column_stats_collector.h"

#include "common/macros.h"

namespace terrier {
namespace optimizer {

ColumnStatsCollector::ColumnStatsCollector(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                           catalog::col_oid_t column_id,
                                           type::TypeId column_type,
                                           std::string column_name)
    : database_id_{database_id},
      table_id_{table_id},
      column_id_{column_id},
      column_type_{column_type},
      column_name_{std::move(column_name)},
      hll_{GetHllPrecision()},
      hist_{GetMaxBins()},
      sketch_{GetSketchWidth()},
      //topk_{sketch_, GetTopK()} {}

ColumnStatsCollector::~ColumnStatsCollector() {}

void ColumnStatsCollector::AddValue(const type::TransientValue &value) {
  if (value.Type() != column_type_) {
    OPTIMIZER_LOG_TRACE("Incompatible value type with expected column stats value type.");
    return;
  }
  total_count_++;
  if (value.Null()) {
    null_count_++;
  } else {
    // Update all stats
    hll_.Update(value);
    hist_.Update(value);
    //topk_.Add(value);
  }
}

double ColumnStatsCollector::GetFracNull() {
  if (total_count_ == 0) {
    OPTIMIZER_LOG_TRACE("Cannot calculate stats for table size 0.");
    return 0;
  }
  return (static_cast<double>(null_count_) / total_count_);
}

}  // namespace optimizer
}