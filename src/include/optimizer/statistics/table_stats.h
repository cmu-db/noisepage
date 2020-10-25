#pragma once

#include <memory>
#include <unordered_map>
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
   * @param num_rows - number of rows in table
   * @param is_base_table - says whether the table is a base table
   * @param col_stats_list - initial list of ColumnStats objects to be inserted in the TableStats object
   */
  TableStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id, size_t num_rows, bool is_base_table,
             const std::vector<ColumnStats> &col_stats_list)
      : database_id_(database_id), table_id_(table_id), num_rows_(num_rows), is_base_table_(is_base_table) {
    for (auto &x : col_stats_list) {  // taking each ColumnStats object and storing it in a map
      column_stats_.emplace(x.ColumnStats::GetColumnID(), std::make_unique<ColumnStats>(x));
    }
  }

  /**
   * Default constructor for deserialization
   */
  TableStats() = default;

  /**
   * Updates the number of rows in the table and all of its columns
   * @param new_num_rows - the new number of rows to update to
   */
  void UpdateNumRows(size_t new_num_rows);

  /**
   * Adds a ColumnStats object to the map of ColumnStats objects
   * @param col_stats - ColumnStats object to add
   * @return whether the ColumnStats object is successfully added
   */
  bool AddColumnStats(std::unique_ptr<ColumnStats> col_stats);

  /**
   * Removes all the ColumnStats objects in the ColumnStats map
   */
  void ClearColumnStats() { column_stats_.clear(); }

  /**
   * Gets the cardinality of a column in the table, given its column id
   * @param column_id - the column oid
   * @return the cardinality of the column
   */
  double GetCardinality(catalog::col_oid_t column_id);

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
  common::ManagedPointer<ColumnStats> GetColumnStats(catalog::col_oid_t column_id);

  /**
   * Removes the ColumnStats object for the given column oid in the ColumnStats map
   * @param column_id - the oid of the column
   * @return whether the ColumnStats object was successfully removed
   */
  bool RemoveColumnStats(catalog::col_oid_t column_id);

  /**
   * Checks to see whether the table is a base table
   * @return whether table is base table
   */
  bool IsBaseTable() const { return is_base_table_; }

  /**
   * Gets the number of rows in the table
   * @return the number of rows
   */
  size_t GetNumRows() const { return num_rows_; }

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
   * tells whether table is a base table
   */
  bool is_base_table_;

  /**
   * stores the ColumnStats objects for the columns in the table
   */
  std::unordered_map<catalog::col_oid_t, std::unique_ptr<ColumnStats>> column_stats_;
};
DEFINE_JSON_HEADER_DECLARATIONS(TableStats);
}  // namespace noisepage::optimizer
