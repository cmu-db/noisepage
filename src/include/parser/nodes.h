#pragma once

#include "libpg_query/nodes.h"

/**
 * A value parsenode as produced by the Postgres parser
 */
using value = struct Value {
  NodeTag type_; /**< tag appropriately (eg. T_String) */
  /**
   * value, as specified via tag
   */
  union ValUnion {
    int32_t ival_; /**< A machine integer */
    char *str_;    /**< string */
  } val_;          /**< value */
};
