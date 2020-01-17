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

/**
* Helper Function for Analyze - calculates the column stats for a particular column
*
* @param database oid of the database containing the table containing the column
* @param table oid of the table containing the column
* @param column id of the column
* @param pointer to the sql table containing the column
* @param pointer to OLAP transaction to scan the column
* @param The number of most frequent values to be collected in the column stats object
* @return column stats object containing the column stats
*/
template <typename T>
terrier::optimizer::ColumnStats CalculateColumnStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                                     catalog::col_oid_t columnID, storage::SqlTable *sql_table,
                                                     terrier::transaction::TransactionContext *scan_txn,
                                                     unsigned mostFrequentNumber);
/**
* Analyzes a table and returns a vector of column stats objects for all its columns
*
* @param database oid of the database containing the table to analyze
* @param table oid of the table to analyze
* @param schema of the table to analuze
* @param pointer to the sql table
* @param pointer to OLAP transaction to scan the column
* @param The number of most frequent values to be collected in the column stats object
* @return vector of column stats object containing the column stats of all columns of the table
*/
std::vector<terrier::optimizer::ColumnStats> Analyze(catalog::db_oid_t database_id, catalog::table_oid_t table_id,
                                                     const catalog::Schema &table_schema, storage::SqlTable *sql_table,
                                                     terrier::transaction::TransactionContext *scan_txn,
                                                     unsigned mostFrequentNumber);
}  // namespace terrier
