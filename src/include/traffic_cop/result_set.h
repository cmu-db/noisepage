#pragma once

#include <algorithm>
#include <string>
#include <vector>
#include "type/transient_value.h"

namespace terrier::tcop {

using Row = std::vector<type::TransientValue>;

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

}  // namespace terrier::tcop
