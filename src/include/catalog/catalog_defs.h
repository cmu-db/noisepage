#pragma once

#include <string>
#include <utility>

#include "common/strong_typedef.h"

namespace terrier::catalog {

constexpr uint32_t NULL_OID = 0;  // error return value
constexpr uint32_t START_OID = 1001;

// in name order
STRONG_TYPEDEF(col_oid_t, uint32_t);
STRONG_TYPEDEF(constraint_oid_t, uint32_t);
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

constexpr col_oid_t INVALID_COLUMN_OID = col_oid_t(NULL_OID);
constexpr constraint_oid_t INVALID_CONSTRAINT_OID = constraint_oid_t(NULL_OID);
constexpr db_oid_t INVALID_DATABASE_OID = db_oid_t(NULL_OID);
constexpr index_oid_t INVALID_INDEX_OID = index_oid_t(NULL_OID);
constexpr indexkeycol_oid_t INVALID_INDEXKEYCOL_OID = indexkeycol_oid_t(NULL_OID);
constexpr namespace_oid_t INVALID_NAMESPACE_OID = namespace_oid_t(NULL_OID);
constexpr table_oid_t INVALID_TABLE_OID = table_oid_t(NULL_OID);
constexpr type_oid_t INVALID_TYPE_OID = type_oid_t(NULL_OID);

constexpr char DEFAULT_DATABASE[] = "terrier";

}  // namespace terrier::catalog
