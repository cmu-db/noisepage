#pragma once

#include "catalog/schema.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {
constexpr table_oid_t SEQUENCE_TABLE_OID = table_oid_t(91);
constexpr index_oid_t SEQUENCE_OID_INDEX_OID = index_oid_t(92);

/*
 * Column names of the form "SEQ[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "SEQ_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t SEQOID_COL_OID = col_oid_t(1);        // INTEGER (pkey)
constexpr col_oid_t SEQRELID_COL_OID = col_oid_t(2);      // INTEGER (fkey: pg_class)
constexpr col_oid_t SEQLASTVAL_COL_OID = col_oid_t(3);    // BIGINT
constexpr col_oid_t SEQSTART_COL_OID = col_oid_t(4);      // BIGINT
constexpr col_oid_t SEQINCREMENT_COL_OID = col_oid_t(5);  // BIGINT
constexpr col_oid_t SEQMAX_COL_OID = col_oid_t(6);        // BIGINT
constexpr col_oid_t SEQMIN_COL_OID = col_oid_t(7);        // BIGINT
constexpr col_oid_t SEQCYCLE_COL_OID = col_oid_t(8);      // BOOLEAN

constexpr col_oid_t SEQTEMPTABLEID_COL_OID = col_oid_t(1);   // INTEGER
constexpr col_oid_t SEQTEMPTABLEVAL_COL_OID = col_oid_t(2);  // BIGINT

constexpr uint8_t NUM_PG_SEQUENCE_COLS = 8;

constexpr std::array<col_oid_t, NUM_PG_SEQUENCE_COLS> PG_SEQUENCE_ALL_COL_OIDS{
    SEQOID_COL_OID,       SEQRELID_COL_OID, SEQLASTVAL_COL_OID, SEQSTART_COL_OID,
    SEQINCREMENT_COL_OID, SEQMAX_COL_OID,   SEQMIN_COL_OID,     SEQCYCLE_COL_OID};
}  // namespace terrier::catalog::postgres
