#pragma once

#include <sqlite3.h>

namespace terrier::traffic_cop{

struct Portal{
  sqlite3_stmt *sqlite_stmt_;

  // Since TransientValue forbids copying, using a pointer is more convenient
  std::shared_ptr<std::vector<type::TransientValue>> params;

};

} // namespace terrier::traffic_cop