#pragma once

#include <sqlite3.h>

namespace terrier::traffic_cop{

struct Portal{
  sqlite3_stmt *sqlite_stmt_;
  std::vector<type::TransientValue> params;

};

} // namespace terrier::traffic_cop