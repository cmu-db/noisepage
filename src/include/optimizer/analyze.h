#include <cfloat>
#include <map>
#include <vector>

#include "main/db_main.h"
#include "optimizer/statistics/column_stats.h"
#include "storage/garbage_collector_thread.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier {

template <typename T>
terrier::optimizer::ColumnStats CalculateColumnStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                                     catalog::col_oid_t columnID, storage::SqlTable *sql_table,
                                                     terrier::transaction::TransactionContext *scan_txn,
                                                     unsigned mostFrequentNumber);

std::vector<terrier::optimizer::ColumnStats> Analyze(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                                     const catalog::Schema &table_schema, storage::SqlTable *sql_table,
                                                     terrier::transaction::TransactionContext *scan_txn,
                                                     unsigned mostFrequentNumber);
}  // namespace terrier
