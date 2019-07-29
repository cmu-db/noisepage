#pragma once

#include "catalog/schema.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

#define INDEX_TABLE_OID table_oid_t(31)
#define INDEX_OID_INDEX_OID index_oid_t(32)
#define INDEX_TABLE_INDEX_OID index_oid_t(33)

/*
 * Column names of the form "IND[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "IND_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define INDOID_COL_OID col_oid_t(1)          // INTEGER (pkey, fkey: pg_class)
#define INDRELID_COL_OID col_oid_t(2)        // INTEGER (fkey: pg_class)
#define INDISUNIQUE_COL_OID col_oid_t(3)     // BOOLEAN
#define INDISPRIMARY_COL_OID col_oid_t(4)    // BOOLEAN
#define INDISEXCLUSION_COL_OID col_oid_t(5)  // BOOLEAN
#define INDIMMEDIATE_COL_OID col_oid_t(6)    // BOOLEAN
#define INDISVALID_COL_OID col_oid_t(7)      // BOOLEAN
#define INDISREADY_COL_OID col_oid_t(8)      // BOOLEAN
#define INDISLIVE_COL_OID col_oid_t(9)       // BOOLEAN
#define PG_INDEX_ALL_COL_OIDS                                                                            \
  {                                                                                                      \
    INDOID_COL_OID, INDRELID_COL_OID, INDISUNIQUE_COL_OID, INDISPRIMARY_COL_OID, INDISEXCLUSION_COL_OID, \
        INDIMMEDIATE_COL_OID, INDISVALID_COL_OID, INDISREADY_COL_OID, INDISLIVE_COL_OID                  \
  }
}  // namespace terrier::catalog::postgres
