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

namespace terrier::catalog::postgres {

constexpr table_oid_t LANGUAGE_TABLE_OID = table_oid_t(71);
constexpr index_oid_t LANGUAGE_OID_INDEX_OID = index_oid_t(72);
constexpr index_oid_t LANGUAGE_NAME_INDEX_OID = index_oid_t(73);


/*
 * Column names of the form "LANG[name]_COL_OID" are present in the PostgreSQL
 * catalog specification and columns of the form "ATT_[name]_COL_OID" are
 * terrier-specific addtions (generally pointers to internal objects).
 */
constexpr col_oid_t LANGOID_COL_OID = col_oid_t(1);     // INTEGER (pkey) [col_oid_t]
constexpr col_oid_t LANGNAME_COL_OID = col_oid_t(2);    // VARCHAR (skey) [table_oid_t]

constexpr uint8_t NUM_PG_LANGUAGE_COLS = 2;

constexpr std::array<col_oid_t, NUM_PG_LANGUAGE_COLS> PG_LANGUAGE_ALL_COL_OIDS = {
    LANGOID_COL_OID, LANGNAME_COL_OID};

}  // namespace terrier::catalog::postgres
