#pragma once
// Following the postgres 12 documentation pg_constraint
// https://www.postgresql.org/docs/12/catalog-pg-constraint.html
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

constexpr table_oid_t CONSTRAINT_TABLE_OID = table_oid_t(61);
constexpr index_oid_t CONSTRAINT_OID_INDEX_OID = index_oid_t(62);           // CONOID
constexpr index_oid_t CONSTRAINT_NAME_INDEX_OID = index_oid_t(63);          // CONNAME, CONNAMESPACE
constexpr index_oid_t CONSTRAINT_NAMESPACE_INDEX_OID = index_oid_t(64);     // CONNAMESPACEID
constexpr index_oid_t CONSTRAINT_TABLE_INDEX_OID = index_oid_t(65);         // CONRELID
constexpr index_oid_t CONSTRAINT_INDEX_INDEX_OID = index_oid_t(66);         // CONINDID
constexpr index_oid_t CONSTRAINT_FOREIGNTABLE_INDEX_OID = index_oid_t(67);  // CONFRELID

/*
 * Column names of the form "CON[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "CON_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t CONOID_COL_OID = col_oid_t(1);        // INTEGER (pkey)
constexpr col_oid_t CONNAME_COL_OID = col_oid_t(2);       // VARCHAR - name of the constraint
constexpr col_oid_t CONNAMESPACE_COL_OID = col_oid_t(3);  // INTEGER (fkey: pg_namespace) - namespace of the constraint
constexpr col_oid_t CONTYPE_COL_OID = col_oid_t(4);  // CHAR - type of the constraint, expressed in char defined below
constexpr col_oid_t CONDEFERRABLE_COL_OID = col_oid_t(5);  // BOOLEAN - is the constraint deferrable
constexpr col_oid_t CONDEFERRED_COL_OID = col_oid_t(6);    // BOOLEAN - has the constraint deferred by default?
constexpr col_oid_t CONVALIDATED_COL_OID =
    col_oid_t(7);  // BOOLEAN - has the constraint been validated? currently can only be false for FK
constexpr col_oid_t CONRELID_COL_OID =
    col_oid_t(8);  // INTEGER (fkey: pg_class) - table oid of the table this constraint is on
constexpr col_oid_t CONINDID_COL_OID =
    col_oid_t(9);  // INTEGER (fkey: pg_class) - index oid supporting this constraint, if it's a unique, primary key,
                   // foreign key, or exclusion constraint; else 0
constexpr col_oid_t CONPARENTID_COL_OID =
    col_oid_t(10);  // INTEGER (fkey: pg_constraint) - The corresponding constraint in the parent partitioned table, if
                    // this is a constraint in a partition; else 0
constexpr col_oid_t CONFRELID_COL_OID =
    col_oid_t(11);  // INTEGER - (fkey: pg_class) reference table oid of the fk constraint, 0 for other constraints
constexpr col_oid_t CONFUPDTYPE_COL_OID =
    col_oid_t(12);  // CHAR - type of the cascade action when updating reference table row
constexpr col_oid_t CONFDELTYPE_COL_OID =
    col_oid_t(13);  // CHAR - type of the cascade action when deleting reference table row
constexpr col_oid_t CONFMATCHTYPE_COL_OID = col_oid_t(14);  // CHAR - type of matching type for fk
constexpr col_oid_t CONISLOCAL_COL_OID =
    col_oid_t(15);  // BOOLEAN - This constraint is defined locally for the relation. Note that a constraint can be
                    // locally defined and inherited simultaneously.
constexpr col_oid_t CONINHCOUNT_COL_OID =
    col_oid_t(16);  // INTEGER - The number of direct inheritance ancestors this constraint has. A constraint with a
                    // nonzero number of ancestors cannot be dropped nor renamed.
constexpr col_oid_t CONNOINHERIT_COL_OID = col_oid_t(
    17);  // BOOLEAN - 	This constraint is defined locally for the relation. It is a non-inheritable constraint.
constexpr col_oid_t CONKEY_COL_OID =
    col_oid_t(18);  // VARCHAR - [column_oid_t] space separated column id for affected column. src_col for fk
constexpr col_oid_t CONFKEY_COL_OID = col_oid_t(
    19);  // VARCHAR - [column_oid_t] space separated column id for affected column ref_col for fk, empty for other
constexpr col_oid_t CONPFEQOP_COL_OID = col_oid_t(
    20);  // VARCHAR - [] space separated op id If a foreign key, list of the equality operators for PK = FK comparisons
constexpr col_oid_t CONPPEQOP_COL_OID = col_oid_t(
    21);  // VARCHAR - [] space separated op id If a foreign key, list of the equality operators for PK = PK comparisons
constexpr col_oid_t CONFFEQOP_COL_OID = col_oid_t(22);  // VARCHAR - [] space separated op id 	If a foreign key, list
                                                        // of the equality operators for FK = FK comparisons
constexpr col_oid_t CONEXCLOP_COL_OID = col_oid_t(
    23);  // VARCHAR - [] space separated op id If an exclusion constraint, list of the per-column exclusion operators
constexpr col_oid_t CONBIN_COL_OID =
    col_oid_t(24);  // BIGINT - If a check constraint, an internal representation of the expression

constexpr uint8_t NUM_PG_CONSTRAINT_COLS = 24;

constexpr std::array<col_oid_t, NUM_PG_CONSTRAINT_COLS> PG_CONSTRAINT_ALL_COL_OIDS = {
    CONOID_COL_OID,      CONNAME_COL_OID,      CONNAMESPACE_COL_OID, CONTYPE_COL_OID,       CONDEFERRABLE_COL_OID,
    CONDEFERRED_COL_OID, CONVALIDATED_COL_OID, CONRELID_COL_OID,     CONINDID_COL_OID,      CONPARENTID_COL_OID,
    CONFRELID_COL_OID,   CONFUPDTYPE_COL_OID,  CONFDELTYPE_COL_OID,  CONFMATCHTYPE_COL_OID, CONISLOCAL_COL_OID,
    CONINHCOUNT_COL_OID, CONNOINHERIT_COL_OID, CONKEY_COL_OID,       CONFKEY_COL_OID,       CONPFEQOP_COL_OID,
    CONPPEQOP_COL_OID,   CONFFEQOP_COL_OID,    CONEXCLOP_COL_OID,    CONBIN_COL_OID};

enum class ConstraintType : char {
  CHECK = 'c',
  FOREIGN_KEY = 'f',
  PRIMARY_KEY = 'p',
  UNIQUE = 'u',
  TRIGGER = 't',
  EXCLUSION = 'x',
  NOTNULL = 'n'
};

enum class FKActionType : char {
  NOACT = 'a',        // no action
  RESTRICTION = 'r',  // restrict
  CASCADE = 'c',      // cascade
  SETNULL = 'n',      // set null
  SETDEFAULT = 'd',   // set default
  SETINVALID = 'i'    // set invalid
};

enum class FKMatchType : char {
  FULL = 'f',     // fully match when compare
  PARTIAL = 'p',  // partially match when compare
  SIMPLE = 's',   // simple match when compare
};

const char FK_UPDATE = 'u';
const char FK_DELETE = 'd';

// the delimiter for making oid array into varchar for storage
const char VARCHAR_ARRAY_DELIMITER = ' ';
const std::string VARCHAR_ARRAY_DELIMITER_STRING = " ";
const size_t CONBIN_INVALID_PTR = 0;
}  // namespace terrier::catalog::postgres
