#pragma once
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "storage/data_table.h"
#include "storage/storage_defs.h"

namespace terrier::storage {

/**
 * A SqlTable is a thin layer above DataTable that replaces storage layer concepts like BlockLayout with SQL layer
 * concepts like Schema. This layer will also handle index maintenance, and possibly constraint checking (confirm when
 * we bring in execution layer).
 */
class SqlTable {
 public:
  /**
   * Constructs a new SqlTable with the given Schema, using the given BlockStore as the source
   * of its storage blocks.
   *
   * @param store the Block store to use.
   * @param schema the initial Schema of this SqlTable
   * @param oid unique identifier for this SqlTable
   */
  SqlTable(BlockStore *const store, catalog::Schema schema, const table_oid_t oid)
      : block_store_(store), schema_(std::move(schema)), oid_(oid) {
    auto layout = StorageUtil::BlockLayoutFromSchema(schema_);
    table_ = new DataTable(block_store_, layout.first, layout_version_t(0));
    column_map_ = layout.second;
  }

  /**
   * Destructs a SqlTable, frees all its members.
   */
  ~SqlTable() { delete table_; }

  /**
   * Materializes a single tuple from the given slot, as visible at the timestamp of the calling txn.
   *
   * @param txn the calling transaction
   * @param slot the tuple slot to read
   * @param out_buffer output buffer. The object should already contain projection list information. @see ProjectedRow.
   * @return true if tuple is visible to this txn and ProjectedRow has been populated, false otherwise
   */
  bool Select(transaction::TransactionContext *const txn, const TupleSlot slot, ProjectedRow *const out_buffer) const {
    return table_->Select(txn, slot, out_buffer);
  }

  /**
   * Update the tuple according to the redo buffer given.
   *
   * @param txn the calling transaction
   * @param slot the slot of the tuple to update.
   * @param redo the desired change to be applied. This should be the after-image of the attributes of interest.
   * @return true if successful, false otherwise
   */
  bool Update(transaction::TransactionContext *const txn, const TupleSlot slot, const ProjectedRow &redo) const {
    // check constraints?
    // update indexes?
    return table_->Update(txn, slot, redo);
  }

  /**
   * Inserts a tuple, as given in the redo, and return the slot allocated for the tuple.
   *
   * @param txn the calling transaction
   * @param redo after-image of the inserted tuple.
   * @return the TupleSlot allocated for this insert, used to identify this tuple's physical location for indexes and
   * such.
   */
  TupleSlot Insert(transaction::TransactionContext *const txn, const ProjectedRow &redo) const {
    // check constraints?
    // update indexes?
    return table_->Insert(txn, redo);
  }

  /**
   * Deletes the given TupleSlot, this will call StageWrite on the provided txn to generate the RedoRecord for delete.
   * @param txn the calling transaction
   * @param slot the slot of the tuple to delete
   * @return true if successful, false otherwise
   */
  bool Delete(transaction::TransactionContext *const txn, const TupleSlot slot) {
    // check constraints?
    // update indexes?
    return table_->Delete(txn, slot);
  }

  /**
   * @return mapping between unique col oids and col ids in the storage layer
   */
  const ColumnMap &ColumnMap() const { return column_map_; }

  /**
   * @return table's unique identifier
   */
  table_oid_t Oid() const { return oid_; }

 private:
  BlockStore *const block_store_;
  const catalog::Schema schema_;
  const table_oid_t oid_;
  DataTable *table_;

  // Replace this with catalog calls in the future
  ColumnMap column_map_;
};
}  // namespace terrier::storage
