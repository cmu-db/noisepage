#pragma once

#include "libpg_query/nodes.h"

/**
 * A value parsenode as produced by the Postgres parser
 */
using value = struct value {
  NodeTag type; /**< tag appropriately (eg. T_String) */
  /**
   * value, as specified via tag
   */
  union ValUnion {
    int32_t ival; /**< A machine integer */
    char *str;    /**< string */
  } val;          /**< value */
};
