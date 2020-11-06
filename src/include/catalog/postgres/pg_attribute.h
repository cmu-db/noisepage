#pragma once

#include <array>

#include "catalog/catalog_defs.h"

namespace noisepage::catalog::postgres {

constexpr table_oid_t COLUMN_TABLE_OID = table_oid_t(41);
constexpr index_oid_t COLUMN_OID_INDEX_OID = index_oid_t(42);
constexpr index_oid_t COLUMN_NAME_INDEX_OID = index_oid_t(43);

/*
 * Column names of the form "ATT[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "ATT_[name]_COL_OID" are
 * noisepage-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t ATTNUM_COL_OID = col_oid_t(1);      // INTEGER (pkey) [col_oid_t]
constexpr col_oid_t ATTRELID_COL_OID = col_oid_t(2);    // INTEGER (fkey: pg_class) [table_oid_t]
constexpr col_oid_t ATTNAME_COL_OID = col_oid_t(3);     // VARCHAR
constexpr col_oid_t ATTTYPID_COL_OID = col_oid_t(4);    // INTEGER (fkey: pg_type) [type_oid_t]
constexpr col_oid_t ATTLEN_COL_OID = col_oid_t(5);      // SMALLINT
constexpr col_oid_t ATTNOTNULL_COL_OID = col_oid_t(6);  // BOOLEAN
// The following columns come from 'pg_attrdef' but are included here for
// simplicity.  PostgreSQL splits out the table to allow more fine-grained
// locking during DDL operations which is not an issue in this system
constexpr col_oid_t ADSRC_COL_OID = col_oid_t(7);  // VARCHAR

constexpr uint8_t NUM_PG_ATTRIBUTE_COLS = 7;

constexpr std::array<col_oid_t, NUM_PG_ATTRIBUTE_COLS> PG_ATTRIBUTE_ALL_COL_OIDS = {
    ATTNUM_COL_OID, ATTRELID_COL_OID,   ATTNAME_COL_OID, ATTTYPID_COL_OID,
    ATTLEN_COL_OID, ATTNOTNULL_COL_OID, ADSRC_COL_OID};

}  // namespace noisepage::catalog::postgres
