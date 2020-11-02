#pragma once

#include <array>

#include "catalog/catalog_defs.h"

namespace noisepage::catalog::postgres {

constexpr table_oid_t CONSTRAINT_TABLE_OID = table_oid_t(61);
constexpr index_oid_t CONSTRAINT_OID_INDEX_OID = index_oid_t(62);
constexpr index_oid_t CONSTRAINT_NAME_INDEX_OID = index_oid_t(63);
constexpr index_oid_t CONSTRAINT_NAMESPACE_INDEX_OID = index_oid_t(64);
constexpr index_oid_t CONSTRAINT_TABLE_INDEX_OID = index_oid_t(65);
constexpr index_oid_t CONSTRAINT_INDEX_INDEX_OID = index_oid_t(66);
constexpr index_oid_t CONSTRAINT_FOREIGNTABLE_INDEX_OID = index_oid_t(67);

/*
 * Column names of the form "CON[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "CON_[name]_COL_OID" are
 * noisepage-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t CONOID_COL_OID = col_oid_t(1);         // INTEGER (pkey)
constexpr col_oid_t CONNAME_COL_OID = col_oid_t(2);        // VARCHAR
constexpr col_oid_t CONNAMESPACE_COL_OID = col_oid_t(3);   // INTEGER (fkey: pg_namespace)
constexpr col_oid_t CONTYPE_COL_OID = col_oid_t(4);        // CHAR
constexpr col_oid_t CONDEFERRABLE_COL_OID = col_oid_t(5);  // BOOLEAN
constexpr col_oid_t CONDEFERRED_COL_OID = col_oid_t(6);    // BOOLEAN
constexpr col_oid_t CONVALIDATED_COL_OID = col_oid_t(7);   // BOOLEAN
constexpr col_oid_t CONRELID_COL_OID = col_oid_t(8);       // INTEGER (fkey: pg_class)
constexpr col_oid_t CONINDID_COL_OID = col_oid_t(9);       // INTEGER (fkey: pg_class)
constexpr col_oid_t CONFRELID_COL_OID = col_oid_t(10);     // INTEGER (fkey: pg_class)
constexpr col_oid_t CONBIN_COL_OID = col_oid_t(11);        // BIGINT (assumes 64-bit pointers)
constexpr col_oid_t CONSRC_COL_OID = col_oid_t(12);        // VARCHAR

constexpr uint8_t NUM_PG_CONSTRAINT_COLS = 12;

constexpr std::array<col_oid_t, NUM_PG_CONSTRAINT_COLS> PG_CONSTRAINT_ALL_COL_OIDS = {
    CONOID_COL_OID,        CONNAME_COL_OID,     CONNAMESPACE_COL_OID, CONTYPE_COL_OID,
    CONDEFERRABLE_COL_OID, CONDEFERRED_COL_OID, CONVALIDATED_COL_OID, CONRELID_COL_OID,
    CONINDID_COL_OID,      CONFRELID_COL_OID,   CONBIN_COL_OID,       CONSRC_COL_OID};

enum class ConstraintType : char {
  CHECK = 'c',
  FOREIGN_KEY = 'f',
  PRIMARY_KEY = 'p',
  UNIQUE = 'u',
  TRIGGER = 't',
  EXCLUSION = 'x',
};

}  // namespace noisepage::catalog::postgres
