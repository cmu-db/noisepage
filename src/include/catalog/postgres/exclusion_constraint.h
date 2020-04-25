#pragma once

#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

constexpr table_oid_t EXCLUSION_TABLE_OID = table_oid_t(111);
constexpr index_oid_t EXCLUSION_OID_INDEX_OID = index_oid_t(112);

/*
 * Column names of the form "CON[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "CON_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t EXCLUSIONID_COL_OID = col_oid_t(1);           // INTEGER (pkey)
constexpr col_oid_t EXCLUSIONCONID_COL_OID = col_oid_t(2);        // INTEGER (fkey: pg_constraint) - constraint oid
constexpr col_oid_t EXCLUSIONREFTABLE_COL_OID = col_oid_t(3);     // INTEGER (fkey: pg_class) - table oid of the table the fk is referencing to
constexpr col_oid_t EXCLUSIONCHILDTABLE_COL_OID = col_oid_t(4);   // INTEGER (fkey: pg_class) - table oid of the table declare foreign key (child table)
constexpr col_oid_t EXCLUSIONREFCOL_COL_OID = col_oid_t(5);       // INTEGER - column oid of the fk reference column
constexpr col_oid_t EXCLUSIONCHILDCOL_COL_OID = col_oid_t(6);     // INTEGER - column oid of the column that declare foreign key (child column)

constexpr uint8_t NUM_EXCLUSION_CONSTRAINT_COLS = 6;

constexpr std::array<col_oid_t, NUM_EXCLUSION_CONSTRAINT_COLS> EXCLUSION_CONSTRAINT_ALL_COL_OIDS = {
    EXCLUSIONID_COL_OID,        EXCLUSIONCONID_COL_OID,     EXCLUSIONREFTABLE_COL_OID, EXCLUSIONCHILDTABLE_COL_OID,
    EXCLUSIONREFCOL_COL_OID, EXCLUSIONCHILDCOL_COL_OID};
}  // namespace terrier::catalog::postgres