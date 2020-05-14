#pragma once

#include "catalog/schema.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog::postgres {
constexpr table_oid_t SCHEMA_TABLE_OID = table_oid_t(91);
constexpr index_oid_t SCHEMA_TABLE_VERSION_INDEX_OID = index_oid_t(92);

constexpr col_oid_t SCH_REL_OID_COL_OID = col_oid_t(1);  // INTEGER
constexpr col_oid_t SCH_VERS_COL_OID = col_oid_t(2);     // SMALLINT (ref to REL_VERS_COL_OID in pg_class)
constexpr col_oid_t SCH_PTR_COL_OID = col_oid_t(3);      // BIGINT (assumes 64-bit pointer)

constexpr uint8_t NUM_PG_SCHEMA_COLS = 3;
constexpr std::array<col_oid_t, NUM_PG_SCHEMA_COLS> PG_SCHEMA_ALL_COL_OIDS = {SCH_REL_OID_COL_OID, SCH_VERS_COL_OID,
                                                                              SCH_PTR_COL_OID};

}  // namespace terrier::catalog::postgres
