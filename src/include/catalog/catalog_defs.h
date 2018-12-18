#pragma once
#include "common/strong_typedef.h"

namespace terrier::catalog {
// refer to catalog/catalog.h for OID numbering scheme
#define DEFAULT_COL_OID col_oid_t(1)
#define DEFAULT_TABLE_OID table_oid_t(1)
#define DEFAULT_DATABASE_OID db_oid_t(1)
#define PG_CATALOG_OID namespace_oid_t(1)

// namespace oid macros
#define USER_NAMESPACE_START_OID namespace_oid_t(100)

// table oid macros
#define DATABASE_CATALOG_TABLE_START_OID table_oid_t(1000)
#define USER_TABLE_START_OID table_oid_t(10000)

// column oid macros
#define DATABASE_CATALOG_COL_START_OID col_oid_t(1000)
#define USER_COL_START_OID col_oid_t(10000)

STRONG_TYPEDEF(col_oid_t, uint32_t);
STRONG_TYPEDEF(table_oid_t, uint32_t);
STRONG_TYPEDEF(db_oid_t, uint32_t);
STRONG_TYPEDEF(namespace_oid_t, uint32_t);
}  // namespace terrier::catalog
