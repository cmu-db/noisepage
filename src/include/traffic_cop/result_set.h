#pragma once

#include <algorithm>
#include <string>
#include <vector>

namespace terrier::traffic_cop {

using Row = std::vector<std::string>;

/**
 * The result set for a query
 */

class ResultSet {
 public:
  /**
   * The column names
   */
  std::vector<std::string> column_names_;

  /**
   * The rows
   */
  std::vector<Row> rows_;
};

}  // namespace terrier::traffic_cop
