#pragma once

#include <array>

#include "catalog/catalog_defs.h"

namespace terrier::catalog::postgres {

constexpr table_oid_t CONSTRAINT_TABLE_OID = table_oid_t(61);
constexpr index_oid_t CONSTRAINT_OID_INDEX_OID = index_oid_t(62);
constexpr index_oid_t CONSTRAINT_NAME_INDEX_OID = index_oid_t(63);
constexpr index_oid_t CONSTRAINT_NAMESPACE_INDEX_OID = index_oid_t(64);
constexpr index_oid_t CONSTRAINT_TABLE_INDEX_OID = index_oid_t(65);
constexpr index_oid_t CONSTRAINT_INDEX_INDEX_OID = index_oid_t(66);
// constexpr index_oid_t CONSTRAINT_FOREIGNTABLE_INDEX_OID = index_oid_t(67);

/*
 * Column names of the form "CON[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "CON_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t CONOID_COL_OID = col_oid_t(1);         // INTEGER (pkey)
constexpr col_oid_t CONNAME_COL_OID = col_oid_t(2);        // VARCHAR - name of the constraint
constexpr col_oid_t CONNAMESPACE_COL_OID = col_oid_t(3);   // INTEGER (fkey: pg_namespace) - namespace of the constraint
constexpr col_oid_t CONTYPE_COL_OID = col_oid_t(4);        // CHAR - type of the constraint, expressed in char defined below
constexpr col_oid_t CONDEFERRABLE_COL_OID = col_oid_t(5);  // BOOLEAN - is the constraint deferrable
constexpr col_oid_t CONDEFERRED_COL_OID = col_oid_t(6);    // BOOLEAN - has the constraint deferred by default?
constexpr col_oid_t CONVALIDATED_COL_OID = col_oid_t(7);   // BOOLEAN - has the constraint been validated? currently can only be false for FK
constexpr col_oid_t CONRELID_COL_OID = col_oid_t(8);       // INTEGER (fkey: pg_class) - table oid of the table this constraint is on
constexpr col_oid_t CONINDID_COL_OID = col_oid_t(9);       // INTEGER (fkey: pg_class) - index oid supporting this constraint, if it's a unique, primary key, foreign key, or exclusion constraint; else 0
constexpr col_oid_t CONFRELID_COL_OID = col_oid_t(10);     // VARCHAR - An array of [constraint_oid_t] fk_constraint_id that this foreign key contains, empty string for other constraints
constexpr col_oid_t CONCOL_COL_OID = col_oid_t(11);        // VARCHAR - An array of [column_oid_t] column id that unique, pk applies to. empty string for other type
constexpr col_oid_t CONCHECK_COL_OID = col_oid_t(12);      // INTEGER (fkey) - row id for the check_constraint_id for the check constraint tof this table, 0 if other type of constraints  
constexpr col_oid_t CONEXCLUSION_COL_OID = col_oid_t(13);  // INTEGER (fkey) - row id for the exclusion_constraint_id for the exclusion constraint tof this table, 0 if other type of constraints  
constexpr col_oid_t CONBIN_COL_OID = col_oid_t(14);        // BIGINT - the expression embedded

constexpr uint8_t NUM_PG_CONSTRAINT_COLS = 14;

constexpr std::array<col_oid_t, NUM_PG_CONSTRAINT_COLS> PG_CONSTRAINT_ALL_COL_OIDS = {
    CONOID_COL_OID,        CONNAME_COL_OID,     CONNAMESPACE_COL_OID, CONTYPE_COL_OID,
    CONDEFERRABLE_COL_OID, CONDEFERRED_COL_OID, CONVALIDATED_COL_OID, CONRELID_COL_OID,
    CONINDID_COL_OID,      CONFRELID_COL_OID,   CONCOL_COL_OID, CONCHECK_COL_OID, CONEXCLUSION_COL_OID, CONBIN_COL_OID};

enum class ConstraintType : char {
  CHECK = 'c',
  FOREIGN_KEY = 'f',
  PRIMARY_KEY = 'p',
  UNIQUE = 'u',
  TRIGGER = 't',
  EXCLUSION = 'x',
  NOTNULL = 'n'
};

// the delimiter for making oid array into varchar for storage
const char VARCHAR_ARRAY_DELIMITER = ' ';
const std::string VARCHAR_ARRAY_DELIMITER_STRING = " ";

}  // namespace terrier::catalog::postgres
