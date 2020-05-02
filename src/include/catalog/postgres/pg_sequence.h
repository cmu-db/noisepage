#pragma once

#include "catalog/schema.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {
    constexpr table_oid_t SEQUENCE_TABLE_OID = table_oid_t(101);
    constexpr index_oid_t SEQUENCE_OID_INDEX_OID = index_oid_t(102);
    constexpr index_oid_t SEQUENCE_NAME_INDEX_OID = index_oid_t(103);

/*
 * Column names of the form "SEQ[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "SEQ_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
    constexpr col_oid_t SEQOID_COL_OID = col_oid_t(1);   // INTEGER (pkey)
    constexpr col_oid_t SEQRELID_COL_OID = col_oid_t(2); // INTEGER (fkey: pg_class)

    constexpr uint8_t NUM_PG_SEQUENCE_COLS = 2;

    constexpr std::array<col_oid_t, NUM_PG_SEQUENCE_COLS> PG_SEQUENCE_ALL_COL_OIDS{SEQOID_COL_OID, SEQRELID_COL_OID};
}  // namespace terrier::catalog::postgres


class SequenceMetadata {
private:
    int64_t curr_value = 0;
public:
    int64_t nextval(){
        curr_value ++;
        return curr_value;
    }
};