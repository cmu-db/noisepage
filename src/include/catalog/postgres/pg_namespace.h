#pragma once

#include "catalog/schema.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {
#define NAMESPACE_TABLE_OID table_oid_t(11)
#define NAMESPACE_OID_INDEX_OID index_oid_t(12)
#define NAMESPACE_NAME_INDEX_OID index_oid_t(13)
#define NAMESPACE_CATALOG_NAMESPACE_OID namespace_oid_t(14)
#define NAMESPACE_DEFAULT_NAMESPACE_OID namespace_oid_t(15)
/*
 * Column names of the form "NSP[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "NSP_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define NSPOID_COL_OID col_oid_t(1)   // INTEGER (pkey)
#define NSPNAME_COL_OID col_oid_t(2)  // VARCHAR
}  // namespace terrier::catalog::postgres
