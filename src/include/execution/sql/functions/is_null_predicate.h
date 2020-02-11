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
   * @return true iff val is null
   */
  static bool IsNull(const Val &val) { return val.is_null_; }

  /**
   * @return true iff val is not null
   */
  static bool IsNotNull(const Val &val) { return !val.is_null_; }
};

}  // namespace terrier::execution::sql
