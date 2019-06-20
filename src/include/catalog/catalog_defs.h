#pragma once

#include <string>
#include <utility>

#include "common/strong_typedef.h"

namespace terrier::catalog {

#define NULL_OID 0  // error return value
#define START_OID 1001

#define INVALID_COLUMN_OID col_oid_t(NULL_OID)
#define INVALID_DATABASE_OID db_oid_t(NULL_OID)
#define INVALID_INDEX_OID index_oid_t(NULL_OID)
#define INVALID_INDEXKEYCOL_OID indexkeycol_oid_t(NULL_OID)
#define INVALID_NAMESPACE_OID namespace_oid_t(NULL_OID)
#define INVALID_TABLE_OID table_oid_t(NULL_OID)
#define INVALID_TYPE_OID type_oid_t(NULL_OID)
#define INVALID_CONSTRAINT_OID constraint_oid_t(NULL_OID)

#define DEFAULT_DATABASE "terrier"

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
STRONG_TYPEDEF(constraint_oid_t, uint32_t);

}  // namespace terrier::catalog
