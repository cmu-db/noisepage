#pragma once

#include "common/strong_typedef.h"

namespace terrier::catalog {
STRONG_TYPEDEF(col_oid_t, uint32_t);
STRONG_TYPEDEF(db_oid_t, uint32_t);
STRONG_TYPEDEF(namespace_oid_t, uint32_t);
STRONG_TYPEDEF(index_oid_t, uint32_t);
STRONG_TYPEDEF(indexkeycol_oid_t, uint32_t);
STRONG_TYPEDEF(table_oid_t, uint32_t);
STRONG_TYPEDEF(trigger_oid_t, uint32_t);
STRONG_TYPEDEF(view_oid_t, uint32_t);
}  // namespace terrier::catalog
