#pragma once

#include "execution/sql/value.h"

namespace noisepage::execution::sql {

/**
 * Utility class to check NULL-ness of SQL values.
 */
class EXPORT IsNullPredicate {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(IsNullPredicate);

  /**
   * @return true iff val is null
   */
  static bool IsNull(const Val &val) { return val.is_null_; }

  /**
   * @return true iff val is not null
   */
  static bool IsNotNull(const Val &val) { return !val.is_null_; }
};

}  // namespace noisepage::execution::sql
