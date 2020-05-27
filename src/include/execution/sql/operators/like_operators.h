#pragma once

#include <cstdlib>

#include "execution/sql/runtime_types.h"

namespace terrier::execution::sql {

static constexpr const char DEFAULT_ESCAPE = '\\';

/**
 * Functor implementing the SQL LIKE() operator
 */
struct Like {
  /**
   * Check if @p str is like @p pattern.
   */
  static bool Apply(const char *str, std::size_t str_len, const char *pattern, std::size_t pattern_len,
                    char escape = DEFAULT_ESCAPE);

  /**
   * Check if @p str is like @p pattern.
   */
  static bool Apply(const storage::VarlenEntry &str, const storage::VarlenEntry &pattern,
                    char escape = DEFAULT_ESCAPE) {
    return Apply(reinterpret_cast<const char *>(str.Content()), str.Size(),
                 reinterpret_cast<const char *>(pattern.Content()), pattern.Size(), escape);
  }
};

/**
 * Functor implementing the SQL NOT LIKE() operator
 */
struct NotLike {
  /**
   * Check if @p str is not like @p pattern.
   */
  static bool Apply(const char *str, std::size_t str_len, const char *pattern, std::size_t pattern_len,
                    char escape = DEFAULT_ESCAPE) {
    return !Like::Apply(str, str_len, pattern, pattern_len, escape);
  }

  /**
   * Check if @p str is not like @p pattern.
   */
  static bool Apply(const storage::VarlenEntry &str, const storage::VarlenEntry &pattern,
                    char escape = DEFAULT_ESCAPE) {
    return !Like::Apply(reinterpret_cast<const char *>(str.Content()), str.Size(),
                        reinterpret_cast<const char *>(pattern.Content()), pattern.Size(), escape);
  }
};

}  // namespace terrier::execution::sql
