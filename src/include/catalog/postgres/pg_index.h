#pragma once

#include <array>

#include "catalog/catalog_defs.h"

namespace noisepage::catalog::postgres {

constexpr table_oid_t INDEX_TABLE_OID = table_oid_t(31);
constexpr index_oid_t INDEX_OID_INDEX_OID = index_oid_t(32);
constexpr index_oid_t INDEX_TABLE_INDEX_OID = index_oid_t(33);

/*
 * Column names of the form "IND[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "IND_[name]_COL_OID" are
 * noisepage-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t INDOID_COL_OID = col_oid_t(1);          // INTEGER (pkey, fkey: pg_class)
constexpr col_oid_t INDRELID_COL_OID = col_oid_t(2);        // INTEGER (fkey: pg_class)
constexpr col_oid_t INDISUNIQUE_COL_OID = col_oid_t(3);     // BOOLEAN
constexpr col_oid_t INDISPRIMARY_COL_OID = col_oid_t(4);    // BOOLEAN
constexpr col_oid_t INDISEXCLUSION_COL_OID = col_oid_t(5);  // BOOLEAN
constexpr col_oid_t INDIMMEDIATE_COL_OID = col_oid_t(6);    // BOOLEAN
constexpr col_oid_t INDISVALID_COL_OID = col_oid_t(7);      // BOOLEAN
constexpr col_oid_t INDISREADY_COL_OID = col_oid_t(8);      // BOOLEAN
constexpr col_oid_t INDISLIVE_COL_OID = col_oid_t(9);       // BOOLEAN
constexpr col_oid_t IND_TYPE_COL_OID = col_oid_t(10);       // CHAR (see IndexSchema)

constexpr uint8_t NUM_PG_INDEX_COLS = 10;

constexpr std::array<col_oid_t, NUM_PG_INDEX_COLS> PG_INDEX_ALL_COL_OIDS = {
    INDOID_COL_OID,       INDRELID_COL_OID,   INDISUNIQUE_COL_OID, INDISPRIMARY_COL_OID, INDISEXCLUSION_COL_OID,
    INDIMMEDIATE_COL_OID, INDISVALID_COL_OID, INDISREADY_COL_OID,  INDISLIVE_COL_OID,    IND_TYPE_COL_OID};
}  // namespace noisepage::catalog::postgres
