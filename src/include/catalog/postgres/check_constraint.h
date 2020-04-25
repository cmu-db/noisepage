#pragma once

#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

constexpr table_oid_t CHECK_TABLE_OID = table_oid_t(101);
constexpr index_oid_t CHECK_OID_INDEX_OID = index_oid_t(102);

/*
 * Column names of the form "CON[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "CON_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t CHECKID_COL_OID = col_oid_t(1);           // INTEGER (pkey)
constexpr col_oid_t CHECKCONID_COL_OID = col_oid_t(2);        // INTEGER (fkey: pg_constraint) - constraint oid
constexpr col_oid_t CHECKREFTABLE_COL_OID = col_oid_t(3);     // INTEGER (fkey: pg_class) - table oid of the table the fk is referencing to
constexpr col_oid_t CHECKCHILDTABLE_COL_OID = col_oid_t(4);   // INTEGER (fkey: pg_class) - table oid of the table declare foreign key (child table)
constexpr col_oid_t CHECKREFCOL_COL_OID = col_oid_t(5);       // INTEGER - column oid of the fk reference column
constexpr col_oid_t CHECKCHILDCOL_COL_OID = col_oid_t(6);     // INTEGER - column oid of the column that declare foreign key (child column)
constexpr col_oid_t CHECKBIN_COL_OID = col_oid_t(7);          // BIGINT (assumes 64-bit pointers)
constexpr col_oid_t CHECKSRC_COL_OID = col_oid_t(8);          // VARCHAR

constexpr uint8_t NUM_CHECK_CONSTRAINT_COLS = 8;

constexpr std::array<col_oid_t, NUM_CHECK_CONSTRAINT_COLS> CHECK_CONSTRAINT_ALL_COL_OIDS = {
    CHECKID_COL_OID,        CHECKCONID_COL_OID,     CHECKREFTABLE_COL_OID, CHECKCHILDTABLE_COL_OID,
    CHECKREFCOL_COL_OID, CHECKCHILDCOL_COL_OID, CHECKBIN_COL_OID, CHECKSRC_COL_OID};
}  // namespace terrier::catalog::postgres