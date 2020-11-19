#pragma once

#include <algorithm>
#include <cstring>
#include <limits>
#include <unordered_map>
#include <vector>

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "common/shared_latch.h"
#include "storage/projected_columns.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"

namespace noisepage::execution::sql {
class VectorProjection;
}  // namespace noisepage::execution::sql

namespace noisepage::transaction {
class TransactionContext;
class TransactionManager;
}  // namespace noisepage::transaction

namespace noisepage::storage {

namespace index {
class Index;
template <typename KeyType>
class BwTreeIndex;
template <typename KeyType>
class HashIndex;
}  // namespace index

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
    SlotIterator &operator++() {
      RawBlock *b = current_slot_.GetBlock();
      slot_num_++;

      if (LIKELY(slot_num_ < max_slot_num_)) {
        current_slot_ = {b, slot_num_};
      } else {
        NOISEPAGE_ASSERT(block_index_ <= end_index_, "block_index_ must always stay in range of table's size");
        block_index_++;
        UpdateFromNextBlock();
      }
      return *this;
    }

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
    bool operator==(const SlotIterator &other) const { return current_slot_ == other.current_slot_; }

    /**
     * Inequality check.
     * @param other other iterator to compare to
     * @return if the two iterators are not equal
     */
    bool operator!=(const SlotIterator &other) const { return !this->operator==(other); }

   private:
    friend class DataTable;

    // constructor for DataTable::end()
    SlotIterator() = default;

    SlotIterator(const DataTable *table) : table_(table), block_index_(0) {  // NOLINT
      end_index_ = table_->blocks_size_;
      NOISEPAGE_ASSERT(end_index_ >= 1, "there should always be at least one block");
      UpdateFromNextBlock();
    }

    void UpdateFromNextBlock() {
      NOISEPAGE_ASSERT(end_index_ >= 1, "there should always be at least one block");

      while (true) {
        if (UNLIKELY(block_index_ == end_index_)) {
          max_slot_num_ = 0;
          current_slot_ = InvalidTupleSlot();
          return;
        }

        RawBlock *b;
        {
          common::SharedLatch::ScopedSharedLatch latch(&table_->blocks_latch_);
          b = table_->blocks_[block_index_];
        }
        slot_num_ = 0;
        max_slot_num_ = b->GetInsertHead();
        current_slot_ = {b, slot_num_};

        if (max_slot_num_ != 0) return;
        block_index_++;
      }
    }

    static auto InvalidTupleSlot() -> TupleSlot { return {nullptr, 0}; }
    const DataTable *table_ = nullptr;
    uint64_t block_index_ = 0, end_index_ = 0;
    TupleSlot current_slot_ = InvalidTupleSlot();
    uint32_t slot_num_ = 0, max_slot_num_ = 0;
  };
  /**
   * Constructs a new DataTable with the given layout, using the given BlockStore as the source
   * of its storage blocks. The first column must be size 8 and is effectively hidden from upper levels.
   *
   * @param store the Block store to use.
   * @param layout the initial layout of this DataTable. First 2 columns must be 8 bytes.
   * @param layout_version the layout version of this DataTable
   */
  DataTable(common::ManagedPointer<BlockStore> store, const BlockLayout &layout, layout_version_t layout_version);

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
  bool Select(common::ManagedPointer<transaction::TransactionContext> txn, TupleSlot slot,
              ProjectedRow *out_buffer) const;

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
  void Scan(common::ManagedPointer<transaction::TransactionContext> txn, SlotIterator *start_pos,
            ProjectedColumns *out_buffer) const;

  /**
   * Sequentially scans the table starting from the given iterator(inclusive) and materializes as many tuples as would
   * fit into the given buffer, as visible to the transaction given, according to the format described by the given
   * output buffer. The tuples materialized are guaranteed to be visible and valid, and the function makes best effort
   * to fill the buffer, unless there are no more tuples. The given iterator is mutated to point to one slot passed the
   * last slot scanned in the invocation.
   *
   * @param txn The calling transaction.
   * @param start_pos Iterator to the starting location for the sequential scan.
   * @param out_buffer Output buffer. This buffer is always cleared of old values.
   */
  void Scan(common::ManagedPointer<transaction::TransactionContext> txn, SlotIterator *start_pos,
            execution::sql::VectorProjection *out_buffer) const;

  /**
   * @return the first tuple slot contained in the data table
   */
  SlotIterator begin() const {  // NOLINT for STL name compability
    return {this};
  }

  /**
   * Returns one past the last tuple slot contained in the data table. Note that this is not an accurate number when
   * concurrent accesses are happening, as inserts maybe in flight. However, the number given is always transactionally
   * correct, as any inserts that might have happened is not going to be visible to the calling transaction.
   *
   * @return one past the last tuple slot contained in the data table.
   */

  SlotIterator end() const {  // NOLINT for STL name compability
    return SlotIterator();
  }

  /**
   * Return a SlotIterator that will only cover the blocks in the selected range.
   * @param start The index of the block to start iterating at, starts at 0.
   * @param end The index of the block to stop iterating at, ends at GetNumBlocks().
   * @return SlotIterator that will iterate over only the blocks in the range [start, end).
   */
  SlotIterator GetBlockedSlotIterator(uint32_t start, uint32_t end) const {
    NOISEPAGE_ASSERT(start <= end && end <= blocks_size_, "must have valid index for start and end");
    SlotIterator it(this);
    it.end_index_ = std::min<uint64_t>(it.end_index_, end);
    it.block_index_ = start;

    it.UpdateFromNextBlock();
    return it;
  }

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
  bool Update(common::ManagedPointer<transaction::TransactionContext> txn, TupleSlot slot, const ProjectedRow &redo);

  /**
   * Inserts a tuple, as given in the redo, and update the version chain the link to the given
   * delta record. The slot allocated for the tuple is returned.
   *
   * @param txn the calling transaction
   * @param redo after-image of the inserted tuple. Should not reference col_id 0
   * @return the TupleSlot allocated for this insert, used to identify this tuple's physical location for indexes and
   * such.
   */
  TupleSlot Insert(common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &redo);

  /**
   * Deletes the given TupleSlot, this will call StageDelete on the provided txn to generate the RedoRecord for delete.
   * The rest of the behavior follows Update's behavior.
   * @param txn the calling transaction
   * @param slot the slot of the tuple to delete
   * @return true if successful, false otherwise
   */
  bool Delete(common::ManagedPointer<transaction::TransactionContext> txn, TupleSlot slot);

  /**
   * @return pointer to underlying vector of blocks
   */
  std::vector<RawBlock *> GetBlocks() const {
    common::SharedLatch::ScopedSharedLatch latch(&blocks_latch_);
    return std::vector<RawBlock *>(blocks_.begin(), blocks_.end());
  }

  /**
   * @return read-only view of this DataTable's BlockLayout
   */
  const BlockLayout &GetBlockLayout() const { return accessor_.GetBlockLayout(); }

  /**
   * @return Number of blocks in the data table.
   */
  uint32_t GetNumBlocks() const { return blocks_size_; }

  /** @return Maximum number of blocks in the data table. */
  static uint32_t GetMaxBlocks() { return std::numeric_limits<uint32_t>::max(); }

  /**
   * @return a coarse estimation on the number of tuples in this table
   */
  uint64_t GetNumTuple() const { return GetBlockLayout().NumSlots() * blocks_size_; }

  /**
   * @return Approximate heap usage of the table
   */
  size_t EstimateHeapUsage() const {
    // This is a back-of-the-envelope calculation that could be innacurate. It does not account for the delta chain
    // elements that are actually owned by TransactionContext
    return blocks_size_ * common::Constants::BLOCK_SIZE;
  }

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
  // The access observer provides an abstraction layer between the GC and BlockCompactor
  friend class AccessObserver;
  // The ArrowSerializer utilizes the accessor directly in order to do fast reads on the underlying
  // data to minimize copies and increase efficiency.
  friend class ArrowSerializer;

  /**
   * accessor_ tuple access strategy for DataTable
   */
  const TupleAccessStrategy accessor_;

  std::atomic<uint64_t> blocks_size_ = 0;
  std::atomic<uint64_t> insert_index_ = 0;
  common::ManagedPointer<BlockStore> const block_store_;

  // protected by blocks_latch_
  std::vector<RawBlock *> blocks_;
  mutable common::SharedLatch blocks_latch_;
  const layout_version_t layout_version_;

  // A templatized version for select, so that we can use the same code for both row and column access.
  // the method is explicitly instantiated for ProjectedRow and ProjectedColumns::RowView
  template <class RowType>
  bool SelectIntoBuffer(common::ManagedPointer<transaction::TransactionContext> txn, TupleSlot slot,
                        RowType *out_buffer) const;

  void InsertInto(common::ManagedPointer<transaction::TransactionContext> txn, const ProjectedRow &redo,
                  TupleSlot dest);
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
}  // namespace noisepage::storage
