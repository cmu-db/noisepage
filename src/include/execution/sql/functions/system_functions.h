#pragma once

#include "execution/sql/value.h"

namespace noisepage::execution::exec {
class ExecutionContext;
}

namespace noisepage::execution::sql {

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

}  // namespace noisepage::execution::sql
