#pragma once

#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::storage::index {
class IndexPopulator {
 public:
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
