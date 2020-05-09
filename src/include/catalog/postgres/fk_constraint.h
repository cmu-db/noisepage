#pragma once

#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "parser/parser_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {

constexpr table_oid_t FK_TABLE_OID = table_oid_t(91);
constexpr index_oid_t FK_OID_INDEX_OID = index_oid_t(92);
constexpr index_oid_t FK_CON_OID_INDEX_OID = index_oid_t(93);
constexpr index_oid_t FK_SRC_TABLE_OID_INDEX_OID = index_oid_t(94);
constexpr index_oid_t FK_REF_TABLE_OID_INDEX_OID = index_oid_t(95);

/*
 * Column names of the form "CON[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "CON_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t FKID_COL_OID = col_oid_t(1);           // INTEGER (pkey)
constexpr col_oid_t FKCONID_COL_OID = col_oid_t(2);        // INTEGER (fkey: pg_constraint) - constraint oid
constexpr col_oid_t FKREFTABLE_COL_OID = col_oid_t(3);     // INTEGER (fkey: pg_class) - table oid of the table the fk is referencing to
constexpr col_oid_t FKSRCTABLE_COL_OID = col_oid_t(4);   // INTEGER (fkey: pg_class) - table oid of the table declare foreign key (child table)
constexpr col_oid_t FKREFCOL_COL_OID = col_oid_t(5);       // INTEGER - column oid of the fk reference column
constexpr col_oid_t FKSRCCOL_COL_OID = col_oid_t(6);     // INTEGER - column oid of the column that declare foreign key (child column)
constexpr col_oid_t FKUPDATEACTION_COL_OID = col_oid_t(7); // CHAR - action type oon update
constexpr col_oid_t FKDELETEACTION_COL_OID = col_oid_t(8); // CHAR - action type on delete

constexpr uint8_t NUM_FK_CONSTRAINT_COLS = 8;

constexpr std::array<col_oid_t, NUM_FK_CONSTRAINT_COLS> FK_CONSTRAINT_ALL_COL_OIDS = {
    FKID_COL_OID,    FKCONID_COL_OID,    FKREFTABLE_COL_OID,     FKSRCTABLE_COL_OID, FKREFCOL_COL_OID,
    FKSRCCOL_COL_OID, FKUPDATEACTION_COL_OID, FKDELETEACTION_COL_OID};


}  // namespace terrier::catalog::postgres
