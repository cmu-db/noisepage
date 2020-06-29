#pragma once

#include "execution/sql/value.h"

namespace terrier::execution::exec {
class ExecutionContext;
}

namespace terrier::execution::sql {

/**
 * Utility class to handle SQL system functionality.
 */
class EXPORT SystemFunctions {
 public:
  /**
   * Delete to force only static functions
   */
  SystemFunctions() = delete;

  /**
   * Gets the version of the database
   */
  static void Version(exec::ExecutionContext *ctx, StringVal *result);
};

}  // namespace terrier::execution::sql
