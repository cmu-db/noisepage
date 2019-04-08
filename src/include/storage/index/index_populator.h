#pragma once

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
  static void PopulateIndex(transaction::TransactionContext *txn, const SqlTable &sql_table, const Index &index) {
    const IndexMetadata &metadata = index.metadata_;
    const auto &pr_initializer = metadata.GetProjectedRowInitializer();
    auto *key_buf = common::AllocationUtil::AllocateAligned(pr_initializer.ProjectedRowSize());
    ProjectedRow *key = pr_initializer.InitializeRow(key_buf);

    for (const auto &it : sql_table) {
      if (sql_table.Select(txn, it, key)) {
        index.Insert(*key, it);
      }
    }
    delete[] key_buf;
  }
};
}  // namespace terrier::storage::index
