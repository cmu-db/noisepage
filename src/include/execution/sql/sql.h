#pragma once

#include "execution/util/execution_common.h"

namespace terrier::execution::sql {

/**
 * The possible column encodings
 */
enum class ColumnEncoding : uint8_t {
  None,
  Rle,
  Delta,
  IntegerDict,
  StringDict,
};

/**
 * All possible JOIN types
 */
enum class JoinType : uint8_t { Inner, Outer, Left, Right, Anti, Semi };

}  // namespace terrier::execution::sql
