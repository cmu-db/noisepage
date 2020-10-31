#pragma once

#include <memory>
#include <string>
#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"
#include "type/type_id.h"

namespace noisepage::catalog::postgres {

constexpr table_oid_t STATISTIC_TABLE_OID = table_oid_t(91);
constexpr index_oid_t STATISTIC_OID_INDEX_OID = index_oid_t(92);

/*
 * Column names of the form "STA[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "STA_[name]_COL_OID" are
 * terrier-specific additions (generally pointers to internal objects).
 */
constexpr col_oid_t STARELID_COL_OID = col_oid_t(1);     // INTEGER (fkey: pg_class) [table_oid_t]
constexpr col_oid_t STAATTNUM_COL_OID = col_oid_t(2);    // INTEGER (fkey: pg_attribute) [col_oid_t]
constexpr col_oid_t STANULLFRAC_COL_OID = col_oid_t(3);  // DECIMAL
constexpr col_oid_t STADISTINCT_COL_OID = col_oid_t(4);  // DECIMAL
constexpr col_oid_t STA_NUMROWS_COL_OID = col_oid_t(5);  // INTEGER

constexpr uint8_t NUM_PG_STATISTIC_COLS = 5;

constexpr std::array<col_oid_t, NUM_PG_STATISTIC_COLS> PG_STATISTIC_ALL_COL_OIDS = {
    STARELID_COL_OID, STAATTNUM_COL_OID, STANULLFRAC_COL_OID, STADISTINCT_COL_OID, STA_NUMROWS_COL_OID};

}  // namespace terrier::catalog::postgres