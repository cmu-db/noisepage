#include "optimizer/statistics/table_stats.h"

#include <memory>
#include <utility>

#include "common/json.h"
#include "loggers/optimizer_logger.h"

namespace noisepage::optimizer {

bool TableStats::AddColumnStats(std::unique_ptr<ColumnStatsBase> col_stats) {
  auto it = column_stats_.find(col_stats->GetColumnID());
  if (it != column_stats_.end()) {
    OPTIMIZER_LOG_TRACE("There already exists a ColumnStats object with the same oid.");
    return false;
  }
  column_stats_.insert({col_stats->GetColumnID(), std::move(col_stats)});
  return true;
}

bool TableStats::HasColumnStats(catalog::col_oid_t column_id) const {
  return (column_stats_.find(column_id) != column_stats_.end());
}

common::ManagedPointer<ColumnStatsBase> TableStats::GetColumnStats(catalog::col_oid_t column_id) const {
  auto col_it = column_stats_.find(column_id);
  NOISEPAGE_ASSERT(col_it != column_stats_.end(), "Every valid column should have an associated column stats");
  return common::ManagedPointer<ColumnStatsBase>(col_it->second);
}

std::vector<common::ManagedPointer<ColumnStatsBase>> TableStats::GetColumnStats() const {
  std::vector<common::ManagedPointer<ColumnStatsBase>> column_stats;
  for (const auto &[_, column_stat] : column_stats_) {
    column_stats.emplace_back(common::ManagedPointer(column_stat));
  }
  return column_stats;
}

void TableStats::RemoveColumnStats(catalog::col_oid_t column_id) {
  auto col_it = column_stats_.find(column_id);
  NOISEPAGE_ASSERT(col_it != column_stats_.end(), "Every column should have an associated column stats object");
  column_stats_.erase(col_it);
}

nlohmann::json TableStats::ToJson() const {
  nlohmann::json j;
  j["database_id"] = database_id_;
  j["table_id"] = table_id_;
  j["num_rows"] = num_rows_;
  return j;
}

void TableStats::FromJson(const nlohmann::json &j) {
  database_id_ = j.at("database_id").get<catalog::db_oid_t>();
  table_id_ = j.at("table_id").get<catalog::table_oid_t>();
  num_rows_ = j.at("num_rows").get<size_t>();
}

DEFINE_JSON_BODY_DECLARATIONS(TableStats);

}  // namespace noisepage::optimizer
