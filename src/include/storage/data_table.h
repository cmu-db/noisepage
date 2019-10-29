#pragma once
#include <list>
#include <unordered_map>
#include <vector>
#include "common/performance_counter.h"
#include "storage/projected_columns.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"

namespace terrier::transaction {
class TransactionContext;
class TransactionManager;
}  // namespace terrier::transaction

namespace terrier::storage {

namespace index {
class Index;
template <typename KeyType>
class BwTreeIndex;
template <typename KeyType>
class HashIndex;
}  // namespace index

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
   * Iterator for all the slots, claimed or otherwise, in the data table. This is useful for sequential scans.
   */
  class SlotIterator {
   public:
    /**
     * @return reference to the underlying tuple slot
     */
    const TupleSlot &operator*() const { return current_slot_; }

    /**
     * @return pointer to the underlying tuple slot
     */
    const TupleSlot *operator->() const { return &current_slot_; }

    /**
     * pre-fix increment.
     * @return self-reference after the iterator is advanced
     */
    SlotIterator &operator++();

    /**
     * post-fix increment.
     * @return copy of the iterator equal to this before increment
     */
    SlotIterator operator++(int) {
      SlotIterator copy = *this;
      operator++();
      return copy;
    }

    /**
     * Equality check.
     * @param other other iterator to compare to
     * @return if the two iterators point to the same slot
     */
    bool operator==(const SlotIterator &other) const {
      // TODO(Tianyu): I believe this is enough?
      return current_slot_ == other.current_slot_;
    }

    /**
     * Inequality check.
     * @param other other iterator to compare to
     * @return if the two iterators are not equal
     */
    bool operator!=(const SlotIterator &other) const { return !this->operator==(other); }

   private:
    friend class DataTable;
    /**
     * @warning MUST BE CALLED ONLY WHEN CALLER HOLDS LOCK TO THE LIST OF RAW BLOCKS IN THE DATA TABLE
     */
    SlotIterator(const DataTable *table, std::list<RawBlock *>::const_iterator block, uint32_t offset_in_block)
        : table_(table), block_(block) {
      current_slot_ = {block == table->blocks_.end() ? nullptr : *block, offset_in_block};
    }

    // TODO(Tianyu): Can potentially collapse this information into the RawBlock so we don't have to hold a pointer to
    // the table anymore. Right now we need the table to know how many slots there are in the block
    const DataTable *table_;
    std::list<RawBlock *>::const_iterator block_;
    TupleSlot current_slot_;
  };
  /**
   * Constructs a new DataTable with the given layout, using the given BlockStore as the source
   * of its storage blocks. The first column must be size 8 and is effectively hidden from upper levels.
   *
   * @param store the Block store to use.
   * @param layout the initial layout of this DataTable. First 2 columns must be 8 bytes.
   * @param layout_version the layout version of this DataTable
   */
  DataTable(BlockStore *store, const BlockLayout &layout, layout_version_t layout_version);

  /**
   * Destructs a DataTable, frees all its blocks and any potential varlen entries.
   */
  ~DataTable();

  /**
   * Materializes a single tuple from the given slot, as visible to the transaction given, according to the format
   * described by the given output buffer.
   *
   * @param txn the calling transaction
   * @param slot the tuple slot to read
   * @param out_buffer output buffer. The object should already contain projection list information and should not
   * reference col_id 0
   * @return true if tuple is visible to this txn and ProjectedRow has been populated, false otherwise
   */
  bool Select(transaction::TransactionContext *txn, TupleSlot slot, ProjectedRow *out_buffer) const;

  // TODO(Tianyu): Should this be updated in place or return a new iterator? Does the caller ever want to
  // save a point of scan and come back to it later?
  // Alternatively, we can provide an easy wrapper that takes in a const SlotIterator & and returns a SlotIterator,
  // just like the ++i and i++ dichotomy.
  /**
   * Sequentially scans the table starting from the given iterator(inclusive) and materializes as many tuples as would
   * fit into the given buffer, as visible to the transaction given, according to the format described by the given
   * output buffer. The tuples materialized are guaranteed to be visible and valid, and the function makes best effort
   * to fill the buffer, unless there are no more tuples. The given iterator is mutated to point to one slot passed the
   * last slot scanned in the invocation.
   *
   * @param txn the calling transaction
   * @param start_pos iterator to the starting location for the sequential scan
   * @param out_buffer output buffer. The object should already contain projection list information. This buffer is
   *                   always cleared of old values.
   */
  void Scan(transaction::TransactionContext *txn, SlotIterator *start_pos, ProjectedColumns *out_buffer) const;

  /**
   * @return the first tuple slot contained in the data table
   */
  SlotIterator begin() const {  // NOLINT for STL name compability
    common::SpinLatch::ScopedSpinLatch guard(&blocks_latch_);
    return {this, blocks_.begin(), 0};
  }

  /**
   * Returns one past the last tuple slot contained in the data table. Note that this is not an accurate number when
   * concurrent accesses are happening, as inserts maybe in flight. However, the number given is always transactionally
   * correct, as any inserts that might have happened is not going to be visible to the calling transaction.
   *
   * @return one past the last tuple slot contained in the data table.
   */
  SlotIterator end() const;  // NOLINT for STL name compability

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
   * not reference col_id 0
   * @return true if successful, false otherwise
   */
  bool Update(transaction::TransactionContext *txn, TupleSlot slot, const ProjectedRow &redo);

  /**
   * Inserts a tuple, as given in the redo, and update the version chain the link to the given
   * delta record. The slot allocated for the tuple is returned.
   *
   * @param txn the calling transaction
   * @param redo after-image of the inserted tuple. Should not reference col_id 0
   * @return the TupleSlot allocated for this insert, used to identify this tuple's physical location for indexes and
   * such.
   */
  TupleSlot Insert(transaction::TransactionContext *txn, const ProjectedRow &redo);

  /**
   * Deletes the given TupleSlot, this will call StageDelete on the provided txn to generate the RedoRecord for delete.
   * The rest of the behavior follows Update's behavior.
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

  /**
   * @return read-only view of this DataTable's BlockLayout
   */
  const BlockLayout &GetBlockLayout() const { return accessor_.GetBlockLayout(); }

 private:
  // The GarbageCollector needs to modify VersionPtrs when pruning version chains
  friend class GarbageCollector;
  // The TransactionManager needs to modify VersionPtrs when rolling back aborts
  friend class transaction::TransactionManager;
  // The index wrappers need access to IsVisible and HasConflict
  friend class index::Index;
  template <typename KeyType>
  friend class index::BwTreeIndex;
  template <typename KeyType>
  friend class index::HashIndex;
  // The block compactor elides transactional protection in the gather/compression phase and
  // needs raw access to the underlying table.
  friend class BlockCompactor;

  BlockStore *const block_store_;
  const layout_version_t layout_version_;
  const TupleAccessStrategy accessor_;

  // TODO(Tianyu): For now, on insertion, we simply sequentially go through a block and allocate a
  // new one when the current one is full. Needless to say, we will need to revisit this when extending GC to handle
  // deleted tuples and recycle slots
  // TODO(Tianyu): Now that we are switching to a linked list, there probably isn't a reason for it
  // to be latched. Could just easily write a lock-free one if there's performance gain(probably not). vector->list has
  // negligible difference in insert performance (within margin of error) when benchmarked.
  // We also might need our own implementation because we need to handle GC of an unlinked block, as a sequential scan
  // might be on it
  std::list<RawBlock *> blocks_;
  // latch used to protect block list
  mutable common::SpinLatch blocks_latch_;
  // latch used to protect insertion_head_
  mutable common::SpinLatch header_latch_;
  std::list<RawBlock *>::iterator insertion_head_;
  // Check if we need to advance the insertion_head_
  // This function uses header_latch_ to ensure correctness
  void CheckMoveHead(std::list<RawBlock *>::iterator block);
  mutable DataTableCounter data_table_counter_;

  // A templatized version for select, so that we can use the same code for both row and column access.
  // the method is explicitly instantiated for ProjectedRow and ProjectedColumns::RowView
  template <class RowType>
  bool SelectIntoBuffer(transaction::TransactionContext *txn, TupleSlot slot, RowType *out_buffer) const;

  void InsertInto(transaction::TransactionContext *txn, const ProjectedRow &redo, TupleSlot dest);
  // Atomically read out the version pointer value.
  UndoRecord *AtomicallyReadVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor) const;

  // Atomically write the version pointer value. Should only be used by Insert where there is guaranteed to be no
  // contention
  void AtomicallyWriteVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, UndoRecord *desired);

  // Checks for Snapshot Isolation conflicts, used by Update
  bool HasConflict(const transaction::TransactionContext &txn, UndoRecord *version_ptr) const;

  // Wrapper around the other HasConflict for indexes to call (they only have tuple slot, not the version pointer)
  bool HasConflict(const transaction::TransactionContext &txn, TupleSlot slot) const;

  // Performs a visibility check on the designated TupleSlot. Note that this does not traverse a version chain, so this
  // information alone is not enough to determine visibility of a tuple to a transaction. This should be used along with
  // a version chain traversal to determine if a tuple's versions are actually visible to a txn.
  // The criteria for visibility of a slot are presence (slot is occupied) and not deleted
  // (logical delete bitmap is non-NULL).
  bool Visible(TupleSlot slot, const TupleAccessStrategy &accessor) const;

  // Compares and swaps the version pointer to be the undo record, only if its value is equal to the expected one.
  bool CompareAndSwapVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, UndoRecord *expected,
                                UndoRecord *desired);

  // Allocates a new block to be used as insertion head.
  RawBlock *NewBlock();

  /**
   * Determine if a Tuple is visible (present and not deleted) to the given transaction. It's effectively Select's logic
   * (follow a version chain if present) without the materialization. If the logic of Select changes, this should change
   * with it and vice versa.
   * @param txn the calling transaction
   * @param slot the slot of the tuple to check visibility on
   * @return true if tuple is visible to this txn, false otherwise
   */
  bool IsVisible(const transaction::TransactionContext &txn, TupleSlot slot) const;
};
}  // namespace terrier::storage
