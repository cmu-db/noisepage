#pragma once

#include <algorithm>
#include <vector>
#include <string>

namespace terrier::traffic_cop {

using Row = std::vector<std::string>;

class FakeResultSet
{
 public:
  std::vector<std::string> column_names_;
  std::vector<Row> rows_;

};


}