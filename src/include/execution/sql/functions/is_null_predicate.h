#pragma once

#include "execution/sql/value.h"

namespace terrier::sql {

/**
 * Utility class to check NULL-ness of SQL values.
 */
class IsNullPredicate {
 public:
  /**
   * Delete to force only static functions
   */
  IsNullPredicate() = delete;

  /**
   * Set result to true iff val is null
   */
  static void IsNull(BoolVal *result, const Val &val) { *result = BoolVal(val.is_null); }

  /**
   * Set result to true iff val is not null
   */
  static void IsNotNull(BoolVal *result, const Val &val) { *result = BoolVal(!val.is_null); }
};

}  // namespace terrier::sql
