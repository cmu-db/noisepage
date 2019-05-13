#pragma once

#include "common/strong_typedef.h"

namespace terrier::catalog {
// refer to catalog/catalog.h for OID numbering scheme
#define START_OID 1001
#define DEFAULT_DATABASE_OID db_oid_t(1)

// in name order
STRONG_TYPEDEF(col_oid_t, uint32_t);
STRONG_TYPEDEF(db_oid_t, uint32_t);
STRONG_TYPEDEF(index_oid_t, uint32_t);
STRONG_TYPEDEF(indexkeycol_oid_t, uint32_t);
STRONG_TYPEDEF(namespace_oid_t, uint32_t);
STRONG_TYPEDEF(settings_oid_t, uint32_t);
STRONG_TYPEDEF(table_oid_t, uint32_t);
STRONG_TYPEDEF(tablespace_oid_t, uint32_t);
STRONG_TYPEDEF(trigger_oid_t, uint32_t);
STRONG_TYPEDEF(type_oid_t, uint32_t);
STRONG_TYPEDEF(view_oid_t, uint32_t);
}  // namespace terrier::catalog
