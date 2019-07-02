#pragma once

#include "catalog/schema.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

#define TYPE_TABLE_OID table_oid_t(51)
#define TYPE_OID_INDEX_OID index_oid_t(52)
#define TYPE_NAME_INDEX_OID index_oid_t(53)
#define TYPE_NAMESPACE_INDEX_OID index_oid_t(54)

/*
 * Column names of the form "TYP[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "TYP_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define TYPOID_COL_OID col_oid_t(1)        // INTEGER (pkey)
#define TYPNAME_COL_OID col_oid_t(2)       // VARCHAR
#define TYPNAMESPACE_COL_OID col_oid_t(3)  // INTEGER (fkey: pg_namespace)
#define TYPLEN_COL_OID col_oid_t(4)        // SMALLINT
#define TYPBYVAL_COL_OID col_oid_t(5)      // BOOLEAN
#define TYPTYPE_COL_OID col_oid_t(6)       // CHAR

enum class Type : char {
  BASE = 'b',
  COMPOSITE = 'c',
  PG_DOMAIN = 'd',
  ENUM = 'e',
  PSEUDO = 'p',
  RANGE = 'r',
};
}  // namespace terrier::catalog::postgres
