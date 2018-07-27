#pragma once
#include <vector>
#include <unordered_map>
#include "common/concurrent_map.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"

namespace terrier::storage {
// TODO(Tianyu): Move this
class TransactionContext;

struct DeltaRecord {
  DeltaRecord *next_;
  timestamp_t timestamp_;
  ProjectedRow delta_;
};

// Write specified number of bytes to position and interpret the bytes as
// an integer of given size. (Thus only 1, 2, 4, 8 are allowed)
// Truncated if neccessary
void WriteBytes(uint8_t attr_size, uint64_t val, byte *pos) {
  switch (attr_size) {
    case 1: *reinterpret_cast<uint8_t *>(pos) = static_cast<uint8_t>(val);
      break;
    case 2: *reinterpret_cast<uint16_t *>(pos) = static_cast<uint16_t>(val);
      break;
    case 4: *reinterpret_cast<uint32_t *>(pos) = static_cast<uint32_t>(val);
      break;
    case 8: *reinterpret_cast<uint64_t *>(pos) = static_cast<uint64_t>(val);
      break;
    default:
      // Invalid attr size
      throw std::runtime_error("Invalid byte write value");
  }
}

// Read specified number of bytes from position and interpret the bytes as
// an integer of given size. (Thus only 1, 2, 4, 8 are allowed)
uint64_t ReadBytes(uint8_t attr_size, const byte *pos) {
  switch (attr_size) {
    case 1: return *reinterpret_cast<const uint8_t *>(pos);
    case 2: return *reinterpret_cast<const uint16_t *>(pos);
    case 4: return *reinterpret_cast<const uint32_t *>(pos);
    case 8: return *reinterpret_cast<const uint64_t *>(pos);
    default:
      // Invalid attr size
      PELOTON_ASSERT(false);
      return 0;
  }
}

class DataTable {
 public:
  void Select(timestamp_t txn_start_time, TupleSlot slot, ProjectedRow &buffer,
              const std::vector<uint16_t> &projection_list) {
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
    for (uint16_t i = 0; i < buffer.NumColumns(); i++) col_to_index.emplace(buffer.ColumnOffsets()[i], i);
    // Apply deltas until we reconstruct a version safe for us to read
    // If the version chain becomes null, this tuple does not exist for this version, and the last delta
    // record would be an undo for insert that sets the primary key to null, which is intended behavior.
    // TODO(Tianyu): write overloaded comparison operator in StringTypeAlias
    while (version_ptr != nullptr && !version_ptr->timestamp_ >= !txn_start_time) {
      ApplyDelta(accessor.GetBlockLayout(), version_ptr->delta_, buffer, col_to_index);
      version_ptr = version_ptr->next_;
    }
  }

  TupleSlot Insert(TransactionContext *txn, const ProjectedRow &redo) { return TupleSlot(); }

  bool Update(TupleSlot slot, const ProjectedRow &redo, DeltaRecord *undo) {
    auto it = layouts_.Find(slot.GetBlock()->layout_version_);
    PELOTON_ASSERT(it != layouts_.End());
    const TupleAccessStrategy &accessor = it->second;
    DeltaRecord *version_ptr = AtomicallyReadVersionPtr(slot, accessor);
    if (HasConflict(version_ptr, undo)) return false;
    // Either ownable, or the current transaction already owns this slot. We disallow
    // write-write conflcts.
    if (!CompareAnsSwapVersionPtr(slot, accessor, version_ptr, undo)) return false;
    // We have owner ship and before-image of old version; update in place.
    for (uint16_t i = 0; i < redo.NumColumns(); i++) CopyAttrFromProjection(accessor,slot, redo, i);
    return true;
  }

 private:
  // TODO(Tianyu): For now this will only have one element in it until we support concurrent schema.
  ConcurrentMap<layout_version_t, TupleAccessStrategy> layouts_;

  // Get the version chain column. for now always the last column
  uint16_t VersionPtrColumnOffset(const TupleAccessStrategy &accessor) {
    return static_cast<uint16_t>(accessor.GetBlockLayout().num_cols_ - 1);
  }

  void CopyWithNullCheck(const byte *from, ProjectedRow &buffer, uint8_t size, uint16_t offset) {
    if (from == nullptr) buffer.SetNull(offset);
    else WriteBytes(size, ReadBytes(size, from), buffer.AttrForceNotNull(offset));
  }

  void CopyAttrIntoProjection(const TupleAccessStrategy &accessor,
                              TupleSlot slot,
                              ProjectedRow &buffer,
                              uint16_t offset) {
    uint16_t col_offset = buffer.ColumnOffsets()[offset];
    uint8_t attr_size = accessor.GetBlockLayout().attr_sizes_[col_offset];
    byte *stored_attr = accessor.AccessWithNullCheck(slot, col_offset);
    CopyWithNullCheck(stored_attr, buffer, attr_size, offset);
  }

  void CopyAttrFromProjection(const TupleAccessStrategy &accessor,
                              TupleSlot slot,
                              const ProjectedRow &delta,
                              uint16_t offset) {}

  // TODO(Tianyu): This code looks confusing as hell. We should refactor over the weekend.
  void ApplyDelta(const BlockLayout &layout,
                  const ProjectedRow &delta,
                  ProjectedRow &buffer,
                  const std::unordered_map<uint16_t, uint16_t> &col_to_index) {
    for (uint16_t i = 0; i < delta.NumColumns(); i++) {
      uint16_t delta_col_offset = delta.ColumnOffsets()[i];
      auto it = col_to_index.find(delta_col_offset);
      if (it != col_to_index.end()) {
        uint16_t buffer_offset = it->first;
        uint16_t col_id = it->second;
        uint8_t attr_size = layout.attr_sizes_[col_id];
        CopyWithNullCheck(delta.AttrWithNullCheck(i), buffer, attr_size, buffer_offset);
      }
    }
  }

  void ApplyDelta(const TupleAccessStrategy &accessor, const ProjectedRow &delta, TupleSlot slot) {

  }

  DeltaRecord *AtomicallyReadVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor) {
    // TODO(Tianyu): Checking for null really is extra work here.
    // Potentially we can write a specialized method with no null check

    // Here we always access force null because the value of the null bit is irrelevant. (every tuple's
    // version ptr attribute is meaningful).
    byte *ptr_location = accessor.AccessForceNotNull(slot, VersionPtrColumnOffset(accessor));
    return reinterpret_cast<std::atomic<DeltaRecord *> *>(ptr_location)->load();
  }

  bool HasConflict(DeltaRecord *version_ptr, DeltaRecord *undo) {
    return version_ptr != nullptr // Nobody owns this tuple's write lock, no older version visible
        && version_ptr->timestamp_ != undo->timestamp_ // This tuple's write lock is already owned by the txn
        && Uncommitted(version_ptr->timestamp_); // Nobody owns this tuple's write lock, older version still visible
  }

  bool CompareAnsSwapVersionPtr(TupleSlot slot,
                                const TupleAccessStrategy &accessor,
                                DeltaRecord *version_ptr,
                                DeltaRecord *undo) {
    byte *ptr_location = accessor.AccessForceNotNull(slot, VersionPtrColumnOffset(accessor));
    return reinterpret_cast<std::atomic<DeltaRecord *> *>(ptr_location)->compare_exchange_strong(version_ptr, undo);
  }

};
}  // namespace terrier::storage