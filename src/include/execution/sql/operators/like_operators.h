#pragma once

#include <cstdlib>

#include "execution/sql/runtime_types.h"

namespace terrier::execution::sql {

static constexpr const char DEFAULT_ESCAPE = '\\';

/**
 * Functor implementing the SQL LIKE() operator
 */
struct Like {
  static bool Impl(const char *str, std::size_t str_len, const char *pattern, std::size_t pattern_len,
                   char escape = DEFAULT_ESCAPE);

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
  bool operator()(const storage::VarlenEntry &str, const storage::VarlenEntry &pattern,
                  char escape = DEFAULT_ESCAPE) const {
    return !Like {}(str, pattern, escape);
  }
};

}  // namespace terrier::execution::sql
