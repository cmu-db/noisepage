#pragma once

#include <array>

#include "catalog/catalog_defs.h"

namespace noisepage::catalog::postgres {

constexpr table_oid_t TYPE_TABLE_OID = table_oid_t(51);
constexpr index_oid_t TYPE_OID_INDEX_OID = index_oid_t(52);
constexpr index_oid_t TYPE_NAME_INDEX_OID = index_oid_t(53);
constexpr index_oid_t TYPE_NAMESPACE_INDEX_OID = index_oid_t(54);

/*
 * Column names of the form "TYP[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "TYP_[name]_COL_OID" are
 * noisepage-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t TYPOID_COL_OID = col_oid_t(1);        // INTEGER (pkey)
constexpr col_oid_t TYPNAME_COL_OID = col_oid_t(2);       // VARCHAR
constexpr col_oid_t TYPNAMESPACE_COL_OID = col_oid_t(3);  // INTEGER (fkey: pg_namespace)
constexpr col_oid_t TYPLEN_COL_OID = col_oid_t(4);        // SMALLINT
constexpr col_oid_t TYPBYVAL_COL_OID = col_oid_t(5);      // BOOLEAN
constexpr col_oid_t TYPTYPE_COL_OID = col_oid_t(6);       // CHAR

constexpr uint8_t NUM_PG_TYPE_COLS = 6;

constexpr std::array<col_oid_t, NUM_PG_TYPE_COLS> PG_TYPE_ALL_COL_OIDS = {
    TYPOID_COL_OID, TYPNAME_COL_OID, TYPNAMESPACE_COL_OID, TYPLEN_COL_OID, TYPBYVAL_COL_OID, TYPTYPE_COL_OID};

enum class Type : char {
  BASE = 'b',
  COMPOSITE = 'c',
  PG_DOMAIN = 'd',
  ENUM = 'e',
  PSEUDO = 'p',
  RANGE = 'r',
};

// TODO(tanujnay112): Not sure how to do this without hardcoding
constexpr type_oid_t VAR_ARRAY_OID = type_oid_t(11);
}  // namespace noisepage::catalog::postgres
