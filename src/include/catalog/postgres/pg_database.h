#pragma once

#include "catalog/schema.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

#define DATABASE_TABLE_OID table_oid_t(1)
#define DATABASE_OID_INDEX_OID index_oid_t(2)
#define DATABASE_NAME_INDEX_OID index_oid_t(3)

/*
 * Column names of the form "DAT[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "DAT_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define DATOID_COL_OID col_oid_t(1)       // INTEGER (pkey)
#define DATNAME_COL_OID col_oid_t(2)      // VARCHAR
#define DAT_CATALOG_COL_OID col_oid_t(3)  // BIGINT (assumes 64-bit pointers)

}  // namespace terrier::catalog::postgres
