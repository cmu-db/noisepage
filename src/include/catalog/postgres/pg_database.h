#pragma once

#include "catalog/schema.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

constexpr table_oid_t DATABASE_TABLE_OID = table_oid_t(1);
constexpr index_oid_t DATABASE_OID_INDEX_OID = index_oid_t(2);
constexpr index_oid_t DATABASE_NAME_INDEX_OID = index_oid_t(3);

/*
 * Column names of the form "DAT[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "DAT_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t DATOID_COL_OID = col_oid_t(1);       // INTEGER (pkey)
constexpr col_oid_t DATNAME_COL_OID = col_oid_t(2);      // VARCHAR
constexpr col_oid_t DAT_CATALOG_COL_OID = col_oid_t(3);  // BIGINT (assumes 64-bit pointers)

constexpr uint8_t NUM_PG_DATABASE_COLS = 3;

constexpr std::array<col_oid_t, NUM_PG_DATABASE_COLS> PG_DATABASE_ALL_COL_OIDS = {DATOID_COL_OID, DATNAME_COL_OID,
                                                                                  DAT_CATALOG_COL_OID};

}  // namespace terrier::catalog::postgres
