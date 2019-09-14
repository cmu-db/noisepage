#pragma once

#include <sqlite3.h>
#include <memory>
#include <vector>
#include "type/transient_value.h"

namespace terrier::trafficcop {

/**
 * A portal is a statement with bound parameters and is ready to execute.
 */
struct Portal {
  /**
   * The sqlite3 statement
   */
  sqlite3_stmt *sqlite_stmt_;

  /**
   * The sequence of parameter values
   */
  // Since TransientValue forbids copying, using a pointer is more convenient
  std::shared_ptr<std::vector<type::TransientValue>> params_;
};

}  // namespace terrier::trafficcop
