#pragma once

#include <array>

#include "catalog/catalog_defs.h"

namespace noisepage::catalog::postgres {

constexpr table_oid_t CLASS_TABLE_OID = table_oid_t(21);
constexpr index_oid_t CLASS_OID_INDEX_OID = index_oid_t(22);
constexpr index_oid_t CLASS_NAME_INDEX_OID = index_oid_t(23);
constexpr index_oid_t CLASS_NAMESPACE_INDEX_OID = index_oid_t(24);

/*
 * Column names of the form "REL[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "REL_[name]_COL_OID" are
 * noisepage-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t RELOID_COL_OID = col_oid_t(1);          // INTEGER (pkey)
constexpr col_oid_t RELNAME_COL_OID = col_oid_t(2);         // VARCHAR
constexpr col_oid_t RELNAMESPACE_COL_OID = col_oid_t(3);    // INTEGER (fkey: pg_namespace)
constexpr col_oid_t RELKIND_COL_OID = col_oid_t(4);         // CHAR
constexpr col_oid_t REL_SCHEMA_COL_OID = col_oid_t(5);      // BIGINT (assumes 64-bit pointers)
constexpr col_oid_t REL_PTR_COL_OID = col_oid_t(6);         // BIGINT (assumes 64-bit pointers)
constexpr col_oid_t REL_NEXTCOLOID_COL_OID = col_oid_t(7);  // INTEGER

constexpr uint8_t NUM_PG_CLASS_COLS = 7;

constexpr std::array<col_oid_t, NUM_PG_CLASS_COLS> PG_CLASS_ALL_COL_OIDS = {
    RELOID_COL_OID,     RELNAME_COL_OID, RELNAMESPACE_COL_OID,  RELKIND_COL_OID,
    REL_SCHEMA_COL_OID, REL_PTR_COL_OID, REL_NEXTCOLOID_COL_OID};

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

}  // namespace noisepage::catalog::postgres
