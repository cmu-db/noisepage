#pragma once

#include <memory>
#include <string>
#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"
#include "type/type_id.h"

namespace terrier::catalog::postgres {

#define COLUMN_TABLE_OID table_oid_t(41)
#define COLUMN_OID_INDEX_OID index_oid_t(42)
#define COLUMN_NAME_INDEX_OID index_oid_t(43)
#define COLUMN_CLASS_INDEX_OID index_oid_t(44)

/*
 * Column names of the form "ATT[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "ATT_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
#define ATTNUM_COL_OID col_oid_t(1)      // INTEGER (pkey) [col_oid_t]
#define ATTRELID_COL_OID col_oid_t(2)    // INTEGER (fkey: pg_class) [table_oid_t]
#define ATTNAME_COL_OID col_oid_t(3)     // VARCHAR
#define ATTTYPID_COL_OID col_oid_t(4)    // INTEGER (fkey: pg_type) [type_oid_t]
#define ATTLEN_COL_OID col_oid_t(5)      // SMALLINT
#define ATTNOTNULL_COL_OID col_oid_t(6)  // BOOLEAN
// The following columns come from 'pg_attrdef' but are included here for
// simplicity.  PostgreSQL splits out the table to allow more fine-grained
// locking during DDL operations which is not an issue in this system
#define ADSRC_COL_OID col_oid_t(7)  // VARCHAR
#define PG_ATTRIBUTE_ALL_COL_OIDS                                                                            \
  {                                                                                                          \
    ATTNUM_COL_OID, ATTRELID_COL_OID, ATTNAME_COL_OID, ATTTYPID_COL_OID, ATTLEN_COL_OID, ATTNOTNULL_COL_OID, \
        ADSRC_COL_OID                                                                                        \
  }

}  // namespace terrier::catalog::postgres
