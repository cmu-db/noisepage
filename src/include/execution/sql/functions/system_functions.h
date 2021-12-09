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
   * Get the version of the database.
   * @param ctx The execution context
   * @param result The out parameter that receives version string
   */
  static void Version(exec::ExecutionContext *ctx, StringVal *result);

  /**
   * Generate a random floating point value on [0.0, 1.0).
   * @param result The out parameter that receives the result
   */
  static void Random(Real *result);
};

}  // namespace noisepage::execution::sql
