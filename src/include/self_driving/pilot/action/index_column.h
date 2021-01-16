#pragma once

#include <string>
#include <utility>

namespace noisepage::selfdriving::pilot {

/**
 * Store the column information for the CreateIndex (and DropIndex for bookkeeping) action
 * For now only need the column name, but we may augment other information in the future
 */
struct IndexColumn {
  /**
   * Construct an IndexColumn
   * @param column_name name of the column
   */
  explicit IndexColumn(std::string column_name) : column_name_(std::move(column_name)) {}

  /** @return Name of the column */
  const std::string &GetColumnName() const { return column_name_; }

 private:
  std::string column_name_;  // Column name to build index with
};

}  // namespace noisepage::selfdriving::pilot
