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
  static void IsNull(BoolVal *result, const Val &val) { *result = BoolVal(val.is_null_); }

  /**
   * Set result to true iff val is not null
   */
  static void IsNotNull(BoolVal *result, const Val &val) { *result = BoolVal(!val.is_null_); }
};

}  // namespace terrier::execution::sql
