#pragma once

#include <vector>
#include "loggers/main_logger.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"

namespace terrier::storage::index {

/**
 * An index populator is a helper class that populate the latest version of all tuples in the table
 * visible to the current transaction, pack target columns into ProjectedRow's and insert into
 * the given index.
 *
 * This class can only be used when creating an index.
 */
class IndexPopulator {
 public:
  /**
   * The method populates all tuples in the table visible to current transaction with latest version,
   * then pack target columns into ProjectedRow's and insert into the given index.
   *
   * @param txn current transaction context
   * @param sql_table the target sql table
   * @param index target index inserted into
   */
  static void PopulateIndex(transaction::TransactionContext *txn,                                         // NOLINT
                            SqlTable &sql_table, const IndexKeySchema &index_key_schema, Index &index) {  // NOLINT
    // Create the projected row for the index
    const IndexMetadata &metadata = index.GetIndexMetadata();
    const auto &pr_initializer = metadata.GetProjectedRowInitializer();
    auto *key_buf_index = common::AllocationUtil::AllocateAligned(pr_initializer.ProjectedRowSize());
    ProjectedRow *index_key = pr_initializer.InitializeRow(key_buf_index);

    // Create the projected row for the sql table
    std::vector<catalog::col_oid_t> col_oids;
    for (const auto &it : index_key_schema) {
      col_oids.emplace_back(catalog::col_oid_t(!it.GetOid()));
    }
    auto init_and_map = sql_table.InitializerForProjectedRow(col_oids);
    auto *key_buf = common::AllocationUtil::AllocateAligned(init_and_map.first.ProjectedRowSize());
    ProjectedRow *key = init_and_map.first.InitializeRow(key_buf);

    // Record the col_id of each column
    std::vector<col_id_t> sql_table_cols;
    sql_table_cols.reserve(key->NumColumns());
    for (uint16_t i = 0; i < key->NumColumns(); ++i) {
      sql_table_cols.emplace_back(key->ColumnIds()[i]);
    }

    for (const auto &it : sql_table) {
      if (sql_table.Select(txn, it, key)) {
        for (uint16_t i = 0; i < key->NumColumns(); ++i) {
          key->ColumnIds()[i] = index_key->ColumnIds()[i];
        }
        index.Insert(*key, it);
        for (uint16_t i = 0; i < key->NumColumns(); ++i) {
          key->ColumnIds()[i] = sql_table_cols[i];
        }
      }
    }
    delete[] key_buf_index;
    delete[] key_buf;
  }
};
}  // namespace terrier::storage::index
