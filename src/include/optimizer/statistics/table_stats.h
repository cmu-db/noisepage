#pragma once

#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "optimizer/statistics/column_stats.h"

namespace noisepage::optimizer {
/**
 * Represents the statistics of a given table. Stores relevant oids and
 * other important information, as well as a list of all the ColumnStats objects for
 * the columns in the table. Can manipulate its ColumnStats objects (adding/deleting).
 */
class TableStats {
 public:
  /**
   * Constructor
   * @param database_id - database oid of table
   * @param table_id - table oid of table
   * @param col_stats_list - initial list of ColumnStats objects to be inserted in the TableStats object
   */
  TableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
             std::vector<std::unique_ptr<ColumnStatsBase>> *col_stats_list)
      : database_id_(database_id), table_id_(table_id) {
    // Every column should have the same number of rows
    num_rows_ = col_stats_list->empty() ? 0 : (*col_stats_list)[0]->GetNumRows();
    for (auto &col_stat : *col_stats_list) {  // taking each ColumnStats object and storing it in a map
      NOISEPAGE_ASSERT(col_stat->GetNumRows() == num_rows_, "Every column should have the same number of rows");
      column_stats_.emplace(col_stat->GetColumnID(), std::move(col_stat));
    }
  }

  /**
   * Default constructor for deserialization
   */
  TableStats() = default;

  /**
   * Adds a ColumnStats object to the map of ColumnStats objects
   * @param col_stats - ColumnStats object to add
   * @return whether the ColumnStats object is successfully added
   */
  bool AddColumnStats(std::unique_ptr<ColumnStatsBase> col_stats);

  /**
   * Gets the number of columns in the table
   * @return the number of columns
   */
  size_t GetColumnCount() const { return column_stats_.size(); }

  /**
   * Checks to see if there's a ColumnStats object for the given column oid
   * @param column_id - the oid of the column
   * @return whether the ColumnStats object exists
   */
  bool HasColumnStats(catalog::col_oid_t column_id) const;

  /**
   * Retrieves the ColumnStats object for the given column oid in the ColumnStats map
   * @param column_id - the oid of the column
   * @return the pointer to the ColumnStats object
   */
  common::ManagedPointer<ColumnStatsBase> GetColumnStats(catalog::col_oid_t column_id) const;

  /**
   * Retrieves all ColumnStats objects in the ColumnStats map
   * @return pointers to all ColumnStats objects
   */
  std::vector<common::ManagedPointer<ColumnStatsBase>> GetColumnStats() const;

  /**
   * Removes the ColumnStats object for the given column oid in the ColumnStats map
   * @param column_id - the oid of the column
   */
  void RemoveColumnStats(catalog::col_oid_t column_id);

  /**
   * Gets the number of rows in the table
   * @return the number of rows
   */
  size_t GetNumRows() const { return num_rows_; }

  /**
   * Updates the number of rows in the table
   * @param num_rows new number of rows
   */
  void SetNumRows(size_t num_rows) { num_rows_ = num_rows; }

  /**
   * Checks if any of the columns statistics within this table statistics is stale
   * @return true if table statistics contains stale column, false otherwise
   */
  bool HasStaleValues() const {
    return std::any_of(column_stats_.begin(), column_stats_.end(), [](const auto &it) { return it.second->IsStale(); });
  }

  /**
   * Serializes a table stats object
   * @return table stats object serialized to json
   */
  nlohmann::json ToJson() const;
  /**
   * Deserializes a table stats object
   * @param j - serialized table stats object
   */
  void FromJson(const nlohmann::json &j);

 private:
  /**
   * database oid
   */
  catalog::db_oid_t database_id_;

  /**
   * table oid
   */
  catalog::table_oid_t table_id_;

  /**
   * number of rows in table
   */
  size_t num_rows_;

  /**
   * stores the ColumnStats objects for the columns in the table
   */
  std::unordered_map<catalog::col_oid_t, std::unique_ptr<ColumnStatsBase>> column_stats_;
};
DEFINE_JSON_HEADER_DECLARATIONS(TableStats);
}  // namespace noisepage::optimizer
