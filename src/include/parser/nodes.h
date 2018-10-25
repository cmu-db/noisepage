#pragma once

#include "libpg_query/nodes.h"

typedef struct value {
  NodeTag type; /* tag appropriately (eg. T_String) */
  union ValUnion {
    int32_t ival; /* machine integer */
    char *str;    /* string */
  } val;
} value;
