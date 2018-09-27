#pragma once
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "storage/data_table.h"
#include "storage/storage_defs.h"

namespace terrier::storage {
class SqlTable {
 public:
  SqlTable(BlockStore *const store, catalog::Schema schema) : block_store_(store), schema_(std::move(schema)) {
    auto layout = StorageUtil::BlockLayoutFromSchema(schema_);
    table_ = new DataTable(block_store_, layout.first, layout_version_t(0));
    column_map_ = layout.second;
  }
  ~SqlTable() { delete table_; }
  bool Select(transaction::TransactionContext *const txn, const TupleSlot slot, ProjectedRow *const out_buffer) const {
    return table_->Select(txn, slot, out_buffer);
  }
  bool Update(transaction::TransactionContext *const txn, const TupleSlot slot, const ProjectedRow &redo) const {
    // check constraints?
    // update indexes
    return table_->Update(txn, slot, redo);
  }
  TupleSlot Insert(transaction::TransactionContext *const txn, const ProjectedRow &redo) const {
    // check constraints?
    // update indexes
    return table_->Insert(txn, redo);
  }
  bool Delete(transaction::TransactionContext *const txn, const TupleSlot slot) {
    // check constraints?
    // update indexes
    return table_->Delete(txn, slot);
  }

 private:
  BlockStore *const block_store_;
  const catalog::Schema schema_;
  DataTable *table_;

  // Replace this with catalog calls in the future
  ColumnMap column_map_;
};
}  // namespace terrier::storage
