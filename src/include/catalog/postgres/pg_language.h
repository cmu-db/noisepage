#pragma once

#include <array>

#include "catalog/catalog_defs.h"

namespace noisepage::catalog::postgres {

constexpr table_oid_t LANGUAGE_TABLE_OID = table_oid_t(71);
constexpr index_oid_t LANGUAGE_OID_INDEX_OID = index_oid_t(72);
constexpr index_oid_t LANGUAGE_NAME_INDEX_OID = index_oid_t(73);

/*
 * Column names of the form "LAN[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "ATT_[name]_COL_OID" are
 * noisepage-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t LANOID_COL_OID = col_oid_t(1);        // INTEGER (pkey) [lan_oid_t]
constexpr col_oid_t LANNAME_COL_OID = col_oid_t(2);       // VARCHAR (skey)
constexpr col_oid_t LANISPL_COL_OID = col_oid_t(3);       // BOOLEAN (skey)
constexpr col_oid_t LANPLTRUSTED_COL_OID = col_oid_t(4);  // BOOLEAN (skey)

// TODO(tanujnay112): Make these foreign keys when we implement pg_proc
constexpr col_oid_t LANPLCALLFOID_COL_OID = col_oid_t(5);  // INTEGER (skey) (fkey: pg_proc) [proc_oid_t]
constexpr col_oid_t LANINLINE_COL_OID = col_oid_t(6);      // INTEGER (skey) (fkey: pg_proc) [proc_oid_t]
constexpr col_oid_t LANVALIDATOR_COL_OID = col_oid_t(7);   // INTEGER (skey) (fkey: pg_proc) [proc_oid_t]

constexpr uint8_t NUM_PG_LANGUAGE_COLS = 7;

constexpr std::array<col_oid_t, NUM_PG_LANGUAGE_COLS> PG_LANGUAGE_ALL_COL_OIDS = {
    LANOID_COL_OID,        LANNAME_COL_OID,   LANISPL_COL_OID,     LANPLTRUSTED_COL_OID,
    LANPLCALLFOID_COL_OID, LANINLINE_COL_OID, LANVALIDATOR_COL_OID};

constexpr language_oid_t INTERNAL_LANGUAGE_OID = language_oid_t(74);
constexpr language_oid_t PLPGSQL_LANGUAGE_OID = language_oid_t(75);

}  // namespace noisepage::catalog::postgres
