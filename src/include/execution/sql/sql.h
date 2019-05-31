#pragma once

#include "execution/util/common.h"

namespace tpl::sql {

/**
 * The possible column encodings
 */
enum class ColumnEncoding : u8 {
  None,
  Rle,
  Delta,
  IntegerDict,
  StringDict,
};

/**
 * All possible JOIN types
 */
enum class JoinType : u8 { Inner, Outer, Left, Right, Anti, Semi };

}  // namespace tpl::sql
