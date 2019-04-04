#pragma once

#include <sqlite3.h>
#include <vector>
#include "network/postgres_protocol_utils.h"
#include "type/transient_value.h"

namespace terrier::traffic_cop {

struct Statement {
  sqlite3_stmt *sqlite3_stmt_;
  std::vector<type::TypeId> param_types;

  size_t NumParams() { return param_types.size(); }
};

}  // namespace terrier::traffic_cop
