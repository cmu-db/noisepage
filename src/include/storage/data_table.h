#pragma once
#include <unordered_map>
#include <vector>
#include "common/container/concurrent_vector.h"
#include "common/performance_counter.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"

namespace terrier::transaction {
class TransactionContext;
class TransactionManager;
}  // namespace terrier::transaction

namespace terrier::storage {

// clang-format off
#define DataTableCounterMembers(f) \
  f(uint64_t, NumSelect) \
  f(uint64_t, NumUpdate) \
  f(uint64_t, NumInsert) \
  f(uint64_t, NumDelete) \
  f(uint64_t, NumNewBlock)
// clang-format on
DEFINE_PERFORMANCE_CLASS(DataTableCounter, DataTableCounterMembers)
#undef DataTableCounterMembers

/**
 * A DataTable is a thin layer above blocks that handles visibility, schemas, and maintenance of versions for a
 * SQL table. This class should be the main outward facing API for the storage engine. SQL level concepts such
 * as SQL types, varlens and nullabilities are still not meaningful at this level.
 */
class DataTable {
 public:
  /**
   * Constructs a new DataTable with the given layout, using the given BlockStore as the source
   * of its storage blocks. The first 2 columns must be size 8 and are effectively hidden from upper levels.
   *
   * @param store the Block store to use.
   * @param layout the initial layout of this DataTable. First 2 columns must be 8 bytes.
   * @param layout_version the layout version of this DataTable
   */
  DataTable(BlockStore *store, const BlockLayout &layout, layout_version_t layout_version);

  /**
   * Destructs a DataTable, frees all its blocks.
   */
  ~DataTable() {
    common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
    for (RawBlock *block : blocks_) block_store_->Release(block);
    delete[] delete_record;
  }

  // TODO(Tianyu): Implement
  /**
   * @return table oid of this data table
   */
  table_oid_t TableOid() const { return table_oid_t{0}; }

  /**
   * Materializes a single tuple from the given slot, as visible at the timestamp.
   *
   * @param txn the calling transaction
   * @param slot the tuple slot to read
   * @param out_buffer output buffer. The object should already contain projection list information. @see ProjectedRow.
   * @return true if tuple is visible to this txn and ProjectedRow has been populated, false otherwise
   */
  bool Select(transaction::TransactionContext *txn, TupleSlot slot, ProjectedRow *out_buffer) const;

  /**
   * Update the tuple according to the redo buffer given, and update the version chain to link to an
   * undo record that is allocated in the txn. The undo record is populated with a before-image of the tuple in the
   * process. Update will only happen if there is no write-write conflict and tuple is visible, otherwise, this is
   * equivalent to a noop and false is returned. If return is false, undo's table pointer is nullptr (used in Abort and
   * GC)
   *
   * @param txn the calling transaction
   * @param slot the slot of the tuple to update.
   * @param redo the desired change to be applied. This should be the after-image of the attributes of interest. Should
   * not reference column ids 0 or 1. Can only reference column id 1 if the redo originated in the DataTable
   * @return true if successful, false otherwise
   */
  bool Update(transaction::TransactionContext *txn, TupleSlot slot, const ProjectedRow &redo);

  /**
   * Inserts a tuple, as given in the redo, and update the version chain the link to the given
   * delta record. The slot allocated for the tuple is returned.
   *
   * @param txn the calling transaction
   * @param redo after-image of the inserted tuple. Should not reference column ids 0 or 1
   * @return the TupleSlot allocated for this insert, used to identify this tuple's physical location for indexes and
   * such.
   */
  TupleSlot Insert(transaction::TransactionContext *txn, const ProjectedRow &redo);

  /**
   * Deletes the given TupleSlot. The rest of the behavior follows Update's behavior.
   * @param txn the calling transaction
   * @param slot the slot of the tuple to delete
   * @return true if successful, false otherwise
   */
  bool Delete(transaction::TransactionContext *txn, TupleSlot slot);

  /**
   * Return a pointer to the performance counter for the data table.
   * @return pointer to the performance counter
   */
  DataTableCounter *GetDataTableCounter() { return &data_table_counter_; }

 private:
  // The GarbageCollector needs to modify VersionPtrs when pruning version chains
  friend class GarbageCollector;
  // The TransactionManager needs to modify VersionPtrs when rolling back aborts
  friend class transaction::TransactionManager;

  BlockStore *const block_store_;
  const layout_version_t layout_version_;
  const TupleAccessStrategy accessor_;

  // for performance in generating initializer for inserts
  const storage::ProjectedRowInitializer insert_record_initializer_;
  // used as the redo ProjectedRow for deletes
  byte *delete_record;

  // TODO(Tianyu): For now, on insertion, we simply sequentially go through a block and allocate a
  // new one when the current one is full. Needless to say, we will need to revisit this when extending GC to handle
  // deleted tuples and recycle slots
  std::vector<RawBlock *> blocks_;
  common::SpinLatch blocks_latch_;
  // to avoid having to grab a latch every time we insert. Failures are very, very infrequent since these
  // only happen when blocks are full, thus we can afford to be optimistic
  std::atomic<RawBlock *> insertion_head_ = nullptr;
  mutable DataTableCounter data_table_counter_;

  // Atomically read out the version pointer value.
  UndoRecord *AtomicallyReadVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor) const;

  // Atomically write the version pointer value. Should only be used by Insert where there is guaranteed to be no
  // contention
  void AtomicallyWriteVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, UndoRecord *desired);

  // Checks for Snapshot Isolation conflicts, used by Update
  bool HasConflict(UndoRecord *version_ptr, const transaction::TransactionContext *txn) const;

  bool Visible(TupleSlot slot, const TupleAccessStrategy &accessor) const;

  // Compares and swaps the version pointer to be the undo record, only if its value is equal to the expected one.
  bool CompareAndSwapVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, UndoRecord *expected,
                                UndoRecord *desired);

  // Allocates a new block to be used as insertion head.
  void NewBlock(RawBlock *expected_val);
};
}  // namespace terrier::storage
