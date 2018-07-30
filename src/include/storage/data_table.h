#pragma once
#include <unordered_map>
#include <vector>
#include "common/container/concurrent_vector.h"
#include "common/concurrent_map.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "storage/storage_utils.h"

// All tuples potentially visible to txns should have a non-null attribute of version vector.
// This is not to be confused with a non-null version vector that has value nullptr (0).
#define VERSION_VECTOR_COLUMN_ID PRESENCE_COLUMN_ID
namespace terrier::storage {
class DataTable {
 public:
  // TODO(Tianyu): Consider taking in some other info to avoid copying layout
  DataTable(BlockStore &store, const BlockLayout &layout) : block_store_(store) {
    layouts_.Emplace(store, layout);
    NewBlock(nullptr);
    PELOTON_ASSERT(insertion_head_ != nullptr);
  }

  ~DataTable() {
    for (auto it = blocks_.Begin(); it != blocks_.End(); ++it)
      block_store_.Release(*it);
  }

  void Select(timestamp_t txn_start_time, TupleSlot slot, ProjectedRow &buffer,
              const std::vector<uint16_t> &projection_list) {
    // Retrieve the access strategy for the block's layout version. It is expected that
    // a valid block is given, thus the access strategy must have already been created.
    auto it = layouts_.Find(slot.GetBlock()->layout_version_);
    PELOTON_ASSERT(it != layouts_.End());

    const TupleAccessStrategy &accessor = it->second;
    // ProjectedRow should always have fewer attributes than the block layout.
    // The version ptr should not be in there.
    PELOTON_ASSERT(buffer.NumColumns() < accessor.GetBlockLayout().num_cols_);

    // These operations don't need to atomic, because so long as we set the version ptr before
    // updating in place, the reader will know if a conflict can potentially happen, and chase the
    // version chain before returning anyway,
    for (uint16_t i = 0; i < buffer.NumColumns(); i++) CopyAttrIntoProjection(accessor, slot, buffer, i);
    // TODO(Tianyu): Potentially we need a memory fence here to make sure the check on version ptr
    // happens after the row is populated. For now, the compiler should not be smart (or rebellious)
    // enough to reorder this operation in or in front of the for loop.
    DeltaRecord *version_ptr = AtomicallyReadVersionPtr(slot, accessor);
    if (version_ptr == nullptr) return;

    // Creates a mapping from col offset to project list index. This allows us to efficiently
    // access columns since deltas can concern a different set of columns when chasing the
    // version chain
    std::unordered_map<uint16_t, uint16_t> col_to_index;
    for (uint16_t i = 0; i < buffer.NumColumns(); i++) col_to_index.emplace(buffer.ColumnIds()[i], i);
    // Apply deltas until we reconstruct a version safe for us to read
    // If the version chain becomes null, this tuple does not exist for this version, and the last delta
    // record would be an undo for insert that sets the primary key to null, which is intended behavior.
    // TODO(Tianyu): write overloaded comparison operator in StringTypeAlias
    while (version_ptr != nullptr && !version_ptr->timestamp_ >= !txn_start_time) {
      ApplyDelta(accessor.GetBlockLayout(), version_ptr->delta_, buffer, col_to_index);
      version_ptr = version_ptr->next_;
    }
  }

  TupleSlot Insert(const ProjectedRow &redo, DeltaRecord *undo) {
    auto it = layouts_.Find(curr_layout_version_);
    PELOTON_ASSERT(it != layouts_.End());
    const TupleAccessStrategy &accessor = it->second;
    TupleSlot result;
    while (true) {
      RawBlock *block = insertion_head_.load();
      if (accessor.Allocate(block, result)) break;
      NewBlock(block);
    }
    // Version_vector would have already been flipped to not null, but won't be in the redo row. We will
    // need to make sure that the new slot always have nullptr for version vector.
    WriteBytes(sizeof(DeltaRecord *), 0, accessor.AccessForceNotNull(result, VERSION_VECTOR_COLUMN_ID));
    bool no_conflict = Update(result, redo, undo);
    PELOTON_ASSERT(no_conflict);
    return result;
  }

  bool Update(TupleSlot slot, const ProjectedRow &redo, DeltaRecord *undo) {
    auto it = layouts_.Find(slot.GetBlock()->layout_version_);
    PELOTON_ASSERT(it != layouts_.End());
    const TupleAccessStrategy &accessor = it->second;

    DeltaRecord *version_ptr = AtomicallyReadVersionPtr(slot, accessor);
    if (HasConflict(version_ptr, undo)) return false;
    // Either ownable, or the current transaction already owns this slot. We disallow
    // write-write conflcts.
    if (!CompareAndSwapVersionPtr(slot, accessor, version_ptr, undo)) return false;
    // We have owner ship and before-image of old version; update in place.
    for (uint16_t i = 0; i < redo.NumColumns(); i++) CopyAttrFromProjection(accessor, slot, redo, i);
    return true;
  }


 private:
  BlockStore &block_store_;
  // TODO(Tianyu): For now this will only have one element in it until we support concurrent schema.
  ConcurrentMap<layout_version_t, TupleAccessStrategy> layouts_;
  // TODO(Tianyu): Again, change when supporting concurrent schema.
  const layout_version_t curr_layout_version_{0};
  // TODO(Tianyu): For now, on insertion, we simply sequentially go through a block and allocate a
  // new one when the current one is full. Needless to say, we will need to revisit this when writing GC.
  common::ConcurrentVector<RawBlock *> blocks_;
  std::atomic<RawBlock *> insertion_head_ = nullptr;

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

  DeltaRecord *AtomicallyReadVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor) {
    // TODO(Tianyu): We can get rid of this and write a "AccessWithoutNullCheck" if this turns out to be
    // an issue (probably not, we are just reading one extra byte.)

    // The check should never fail. (every tuple's version ptr attribute is meaningful).
    byte *ptr_location = accessor.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
    PELOTON_ASSERT(ptr_location != nullptr);
    return reinterpret_cast<std::atomic<DeltaRecord *> *>(ptr_location)->load();
  }

  bool HasConflict(DeltaRecord *version_ptr, DeltaRecord *undo) {
    return version_ptr != nullptr  // Nobody owns this tuple's write lock, no older version visible
           && version_ptr->timestamp_ != undo->timestamp_  // This tuple's write lock is already owned by the txn
           && Uncommitted(version_ptr->timestamp_);  // Nobody owns this tuple's write lock, older version still visible
  }

  bool CompareAndSwapVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, DeltaRecord *version_ptr,
                                DeltaRecord *undo) {
    // The check should never fail. (every tuple's version ptr attribute is meaningful).
    byte *ptr_location = accessor.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
    PELOTON_ASSERT(ptr_location != nullptr);
    return reinterpret_cast<std::atomic<DeltaRecord *> *>(ptr_location)->compare_exchange_strong(version_ptr, undo);
  }

  // TODO(Tianyu): This shouldn't be performance-critical. So maybe use a latch instead of a cmpxchg?
  // This will eliminate retries, which could potentially be an expensive allocate (this is somewhat mitigated
  // by the object pool reuse)
  void NewBlock(RawBlock *expected_val) {
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