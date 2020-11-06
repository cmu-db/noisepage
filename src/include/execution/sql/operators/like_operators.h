#pragma once

#include <cstdlib>

#include "execution/sql/runtime_types.h"

namespace noisepage::execution::sql {

static constexpr const char DEFAULT_ESCAPE = '\\';

/**
 * Functor implementing the SQL LIKE() operator
 */
struct Like {
  /** @return True if the string is like the pattern with the specified escape character. */
  static bool Impl(const char *str, std::size_t str_len, const char *pattern, std::size_t pattern_len,
                   char escape = DEFAULT_ESCAPE);

  /** @return True if str is LIKE pattern with the specified escape character. */
  bool operator()(const storage::VarlenEntry &str, const storage::VarlenEntry &pattern,
                  char escape = DEFAULT_ESCAPE) const {
    return Impl(reinterpret_cast<const char *>(str.Content()), str.Size(),
                reinterpret_cast<const char *>(pattern.Content()), pattern.Size(), escape);
  }
};

/**
 * Functor implementing the SQL NOT LIKE() operator
 */
struct NotLike {
  /** @return True if str is NOT LIKE pattern with the specified escape character. */
  bool operator()(const storage::VarlenEntry &str, const storage::VarlenEntry &pattern,
                  char escape = DEFAULT_ESCAPE) const {
    return !Like{}(str, pattern, escape);  // NOLINT
  }
};

}  // namespace noisepage::execution::sql
