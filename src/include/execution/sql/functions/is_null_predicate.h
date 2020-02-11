#pragma once

#include "execution/sql/value.h"

namespace terrier::execution::sql {

/**
 * Utility class to check NULL-ness of SQL values.
 */
class EXPORT IsNullPredicate {
 public:
  /**
   * Delete to force only static functions
   */
  IsNullPredicate() = delete;

  /**
   * Set result to true iff val is null
   */
  static void IsNull(bool *result, const Val &val) { *result = val.is_null_; }

  /**
   * Set result to true iff val is not null
   */
  static void IsNotNull(bool *result, const Val &val) { *result = !val.is_null_; }
};

}  // namespace terrier::execution::sql
