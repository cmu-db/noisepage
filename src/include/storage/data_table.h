#pragma once
#include <unordered_map>
#include <vector>
#include "common/concurrent_map.h"
#include "common/container/concurrent_vector.h"
#include "storage/storage_defs.h"
#include "storage/storage_utils.h"
#include "storage/tuple_access_strategy.h"

// All tuples potentially visible to txns should have a non-null attribute of version vector.
// This is not to be confused with a non-null version vector that has value nullptr (0).
#define VERSION_VECTOR_COLUMN_ID PRESENCE_COLUMN_ID

namespace terrier::storage {
/**
 * A DataTable is a thin layer above blocks that handles visibility, schemas, and maintainence of versions for a
 * SQL table. This class should be the main outward facing API for the storage engine. SQL level concepts such
 * as SQL types, varlens and nullabilities are still not meaningful at this level.
 */
class DataTable {
 public:
  // TODO(Tianyu): Consider taking in some other info to avoid copying layout
  /**
   * Constructs a new DataTable with the given layout, using the given BlockStore as the source
   * of its storage blocks.
   *
   * @param store the Block store to use.
   * @param layout the initial layout of this DataTable.
   */
  DataTable(BlockStore &store, const BlockLayout &layout) : block_store_(store) {
    layouts_.Emplace(curr_layout_version_, layout);
    NewBlock(nullptr);
    PELOTON_ASSERT(insertion_head_ != nullptr);
  }

  /**
   * Destructs a DataTable, frees all its blocks.
   */
  ~DataTable() {
    for (auto it = blocks_.Begin(); it != blocks_.End(); ++it) block_store_.Release(*it);
  }

  /**
   * Materializes a single tuple from the given slot, as visible at the timestamp.
   *
   * @param txn_start_time the timestamp threshold that the returned projection should be visible at. In practice this
   *                       will just be the start time of the caller transaction.
   * @param slot the tuple slot to read
   * @param buffer output buffer. The object should already contain projection list information. @see ProjectedRow.
   */
  void Select(const timestamp_t txn_start_time, const TupleSlot slot, ProjectedRow &buffer) {
    // Retrieve the access strategy for the block's layout version. It is expected that
    // a valid block is given, thus the access strategy must have already been created.
    auto it = layouts_.Find(slot.GetBlock()->layout_version_);
    PELOTON_ASSERT(it != layouts_.End());
    const TupleAccessStrategy &accessor = it->second;

    // ProjectedRow should always have fewer attributes than the block layout because we will never return the
    // version ptr above the DataTable layer
    // Also, the version ptr should not be in there: it is a concept hidden from callers.
    PELOTON_ASSERT(buffer.NumColumns() < accessor.GetBlockLayout().num_cols_);
    PELOTON_ASSERT(buffer.NumColumns() > 0);

    // Copy the current (most recent) tuple into the projection list. These operations don't need to be atomic,
    // because so long as we set the version ptr before updating in place, the reader will know if a conflict
    // can potentially happen, and chase the version chain before returning anyway,
    for (uint16_t i = 0; i < buffer.NumColumns(); i++) CopyAttrIntoProjection(accessor, slot, buffer, i);

    // TODO(Tianyu): Potentially we need a memory fence here to make sure the check on version ptr
    // happens after the row is populated. For now, the compiler should not be smart (or rebellious)
    // enough to reorder this operation in or in front of the for loop.
    DeltaRecord *version_ptr = AtomicallyReadVersionPtr(slot, accessor);

    // Nullptr in version chain means no version visible to any transaction alive at this point.
    if (version_ptr == nullptr) return;

    // Creates a mapping from col offset to project list index. This allows us to efficiently
    // access columns since deltas can concern a different set of columns when chasing the
    // version chain
    std::unordered_map<uint16_t, uint16_t> projection_list_col_to_index;
    for (uint16_t i = 0; i < buffer.NumColumns(); i++) projection_list_col_to_index.emplace(buffer.ColumnIds()[i], i);

    // Apply deltas until we reconstruct a version safe for us to read
    // If the version chain becomes null, this tuple does not exist for this version, and the last delta
    // record would be an undo for insert that sets the primary key to null, which is intended behavior.
    while (version_ptr != nullptr && version_ptr->timestamp_ >= txn_start_time) {
      ApplyDelta(accessor.GetBlockLayout(), version_ptr->delta_, buffer, projection_list_col_to_index);
      version_ptr = version_ptr->next_;
    }
  }

  /**
   * Update the tuple according to the redo slot given, and update the version chain to link to the given
   * delta record. The delta record is populated with a before-image of the tuple in the process. Update will only
   * happen if there is no write-write conflict, otherwise, this is equivalent to a noop and false is returned,
   *
   * @param slot the slot of the tuple to update.
   * @param redo the desired change to be applied. This should be the after-image of the attributes of interest.
   * @param undo the undo record to maintain and populate. It is expected that the projected row has the same structure
   *             as the redo, but the contents need not be filled beforehand, and will be populated with the
   * before-image after this method returns.
   * @return whether the update is successful.
   */
  bool Update(const TupleSlot slot, const ProjectedRow &redo, DeltaRecord *undo) {
    auto it = layouts_.Find(slot.GetBlock()->layout_version_);
    PELOTON_ASSERT(it != layouts_.End());
    const TupleAccessStrategy &accessor = it->second;

    // They should have the same layout.
    PELOTON_ASSERT(redo.NumColumns() == undo->delta_.NumColumns());
    // TODO(Tianyu): Do we want to also check the column ids and order?

    DeltaRecord *version_ptr = AtomicallyReadVersionPtr(slot, accessor);
    // Since we disallow write-write conflicts, the version vector pointer is essentially an implicit
    // write lock on the tuple.
    if (HasConflict(version_ptr, undo)) return false;
    // TODO(Tianyu): Is it conceivable that the caller would have already obtained the values and don't need this?
    // Populate undo record with the before image of attribute
    for (uint16_t i = 0; i < redo.NumColumns(); i++) CopyAttrIntoProjection(accessor, slot, undo->delta_, i);

    // At this point, either tuple write lock is ownable, or the current transaction already owns this slot.
    if (!CompareAndSwapVersionPtr(slot, accessor, version_ptr, undo)) return false;
    // Update in place with the new value.
    for (uint16_t i = 0; i < redo.NumColumns(); i++) CopyAttrFromProjection(accessor, slot, redo, i);

    return true;
  }

  /**
   * Inserts a tuple, as given in the redo, and update the version chain the link to the given
   * delta record. The slot allocated for the tuple and returned.
   *
   * @param redo after-image of the inserted tuple
   * @param undo the undo record to maintain and populate. It is expected that this simply contains one column of
   *             the table's primary key, set to null (logically deleted) to denote that the tuple did not exist
   *             before.
   * @return the TupleSlot allocated for this insert, used to identify this tuple's physical location in indexes and
   * such.
   */
  TupleSlot Insert(const ProjectedRow &redo, DeltaRecord *undo) {
    auto it = layouts_.Find(curr_layout_version_);
    PELOTON_ASSERT(it != layouts_.End());
    const TupleAccessStrategy &accessor = it->second;

    // Since this is an insert, all the column values should be given in the redo.
    PELOTON_ASSERT(redo.NumColumns() == (accessor.GetBlockLayout().num_cols_ - 1));

    // Attempt to allocate a new tuple from the block we are working on right now.
    // If that block is full, try to request a new block. Because other concurrent
    // inserts could have already created a new block, we need to use compare and swap
    // to change the insertion head. We do not expect this loop to be executed more than
    // twice, but there is technically a possibility for blocks with only a few slots.
    TupleSlot result;
    while (true) {
      RawBlock *block = insertion_head_.load();
      if (accessor.Allocate(block, result)) break;
      NewBlock(block);
    }

    // Version_vector column would have already been flipped to not null, but won't be in the redo given to us. We will
    // need to make sure that the new slot always have nullptr for version vector.
    WriteBytes(sizeof(DeltaRecord *), 0, accessor.AccessForceNotNull(result, VERSION_VECTOR_COLUMN_ID));

    // Once the version vector is installed, the insert is the same as an update where columns from the
    // redo is copied into the block.
    bool no_conflict = Update(result, redo, undo);
    // Expect no conflict because this version should only be visible to this transaction.
    PELOTON_ASSERT(no_conflict);
    return result;
  }

 private:
  BlockStore &block_store_;
  // TODO(Tianyu): For now this will only have one element in it until we support concurrent schema.
  // TODO(Matt): consider a vector instead if lookups are faster
  ConcurrentMap<layout_version_t, TupleAccessStrategy> layouts_;
  // TODO(Tianyu): Again, change when supporting concurrent schema.
  const layout_version_t curr_layout_version_{0};
  // TODO(Tianyu): For now, on insertion, we simply sequentially go through a block and allocate a
  // new one when the current one is full. Needless to say, we will need to revisit this when writing GC.
  common::ConcurrentVector<RawBlock *> blocks_;
  std::atomic<RawBlock *> insertion_head_ = nullptr;

  // Applies a delta to a materialized tuple. This is a matter of copying value in the undo (before-image) into
  // the materialized tuple if present in the materialized projection.
  void ApplyDelta(const BlockLayout &layout, const ProjectedRow &delta, ProjectedRow &buffer,
                  const std::unordered_map<uint16_t, uint16_t> &col_to_index) {
    for (uint16_t i = 0; i < delta.NumColumns(); i++) {
      uint16_t delta_col_id = delta.ColumnIds()[i];
      auto it = col_to_index.find(delta_col_id);
      if (it != col_to_index.end()) {
        uint16_t buffer_offset = it->first;
        uint16_t col_id = it->second;
        uint8_t attr_size = layout.attr_sizes_[col_id];
        CopyWithNullCheck(delta.AccessWithNullCheck(i), buffer, attr_size, buffer_offset);
      }
    }
  }

  // Atomically read out the version pointer value.
  DeltaRecord *AtomicallyReadVersionPtr(const TupleSlot slot, const TupleAccessStrategy &accessor) {
    // TODO(Tianyu): We can get rid of this and write a "AccessWithoutNullCheck" if this turns out to be
    // an issue (probably not, we are just reading one extra byte.)

    byte *ptr_location = accessor.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
    PELOTON_ASSERT(ptr_location != nullptr);
    return reinterpret_cast<std::atomic<DeltaRecord *> *>(ptr_location)->load();
  }

  // If there will be a write-write conflict.
  bool HasConflict(DeltaRecord *version_ptr, DeltaRecord *undo) {
    return version_ptr != nullptr  // Nobody owns this tuple's write lock, no older version visible
           && version_ptr->timestamp_ != undo->timestamp_  // This tuple's write lock is already owned by the txn
           && Uncommitted(version_ptr->timestamp_);  // Nobody owns this tuple's write lock, older version still visible
  }

  // Compares and swaps the version pointer to be the undo record, only if its value is equal to the expected one.
  bool CompareAndSwapVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, DeltaRecord *expected,
                                DeltaRecord *undo) {
    byte *ptr_location = accessor.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
    // The check should never fail. We should only update the version vector for a tuple that is present
    PELOTON_ASSERT(ptr_location != nullptr);
    return reinterpret_cast<std::atomic<DeltaRecord *> *>(ptr_location)->compare_exchange_strong(expected, undo);
  }

  // Allocates a new block to be used as insertion head.
  void NewBlock(RawBlock *expected_val) {
    // TODO(Tianyu): This shouldn't be performance-critical. So maybe use a latch instead of a cmpxchg?
    // This will eliminate retries, which could potentially be an expensive allocate (this is somewhat mitigated
    // by the object pool reuse)
    auto it = layouts_.Find(curr_layout_version_);
    PELOTON_ASSERT(it != layouts_.End());
    const BlockLayout &layout = it->second.GetBlockLayout();
    RawBlock *new_block = block_store_.Get();
    InitializeRawBlock(new_block, layout, curr_layout_version_);
    if (insertion_head_.compare_exchange_strong(expected_val, new_block))
      blocks_.PushBack(new_block);
    else
      // If the compare and exchange failed, another thread might have already allocated a new block
      // We should release this new block and return. The caller would presumably try again on the new
      // block.
      block_store_.Release(new_block);
  }
};
}  // namespace terrier::storage
