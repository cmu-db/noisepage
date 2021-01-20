#pragma once

#include <string>
#include <vector>

#include "self_driving/pilot/action/index_column.h"

namespace noisepage::selfdriving::pilot {

/**
 * Index Action Generation Utility
 */
class IndexActionUtil {
 public:
  /**
   * Generate the index name for an index action
   * @param table_name Table to create index on
   * @param columns Columns to create index on
   * @return The index name
   */
  static std::string GenerateIndexName(const std::string &table_name, const std::vector<IndexColumn> &columns) {
    // Index name is a combination of table name and the column names
    std::string index_name = "automated_index_" + table_name + "_";
    for (auto &column : columns) index_name += column.GetColumnName() + "_";
    return index_name;
  }
};

}  // namespace noisepage::selfdriving::pilot
