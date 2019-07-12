#pragma once

#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

#define CLASS_TABLE_OID table_oid_t(21)
#define CLASS_OID_INDEX_OID index_oid_t(22)
#define CLASS_NAME_INDEX_OID index_oid_t(23)
#define CLASS_NAMESPACE_INDEX_OID index_oid_t(24)

/*
 * Column names of the form "REL[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "REL_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define RELOID_COL_OID col_oid_t(1)          // INTEGER (pkey)
#define RELNAME_COL_OID col_oid_t(2)         // VARCHAR
#define RELNAMESPACE_COL_OID col_oid_t(3)    // INTEGER (fkey: pg_namespace)
#define RELKIND_COL_OID col_oid_t(4)         // CHAR
#define REL_SCHEMA_COL_OID col_oid_t(5)      // BIGINT (assumes 64-bit pointers)
#define REL_PTR_COL_OID col_oid_t(6)         // BIGINT (assumes 64-bit pointers)
#define REL_NEXTCOLOID_COL_OID col_oid_t(7)  // INTEGER
#define PG_CLASS_ALL_COL_OIDS                                                                                    \
  {                                                                                                              \
    RELOID_COL_OID, RELNAME_COL_OID, RELNAMESPACE_COL_OID, RELKIND_COL_OID, REL_SCHEMA_COL_OID, REL_PTR_COL_OID, \
        REL_NEXTCOLOID_COL_OID                                                                                   \
  }

enum class ClassKind : char {
  REGULAR_TABLE = 'r',
  INDEX = 'i',
  SEQUENCE = 'S',  // yes, this really is the only capitalized one. Ask postgres wtf.
  VIEW = 'v',
  MATERIALIZED_VIEW = 'm',
  COMPOSITE_TYPE = 'c',
  TOAST_TABLE = 't',
  FOREIGN_TABLE = 'f',
};

} // namespace terrier::catalog::postgres
