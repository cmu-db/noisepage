#pragma once
#include <vector>
#include "common/concurrent_map.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"

namespace terrier::storage {
// TODO(Tianyu): Move this
class TransactionContext;

class DeltaRecord {
 private:
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
uint64_t ReadBytes(uint8_t attr_size, byte *pos) {
  switch (attr_size) {
    case 1: return *reinterpret_cast<uint8_t *>(pos);
    case 2: return *reinterpret_cast<uint16_t *>(pos);
    case 4: return *reinterpret_cast<uint32_t *>(pos);
    case 8: return *reinterpret_cast<uint64_t *>(pos);
    default:
      // Invalid attr size
      PELOTON_ASSERT(false);
      return 0;
  }
}

class DataTable {
 public:
  void Select(TransactionContext *txn, TupleSlot slot, ProjectedRow *buffer,
              const std::vector<uint16_t> &projection_list) {
    auto it = layouts_.Find(slot.GetBlock()->layout_version_);
    PELOTON_ASSERT(it != layouts_.End());
    const TupleAccessStrategy &accessor = it->second;
    for (uint16_t i = 0; i < buffer->NumColumns(); i++) CopyAttr(accessor, slot, buffer, i);

  }

  TupleSlot Insert(TransactionContext *txn, const ProjectedRow &redo) { return TupleSlot(); }

  void Update(TransactionContext *txn, TupleSlot slot, const ProjectedRow &redo, const DeltaRecord &undo) {}

 private:
  // TODO(Tianyu): For now this will only have one element in it until we support concurrent schema.
  ConcurrentMap<layout_version_t, TupleAccessStrategy> layouts_;

  void CopyAttr(const TupleAccessStrategy &accessor, TupleSlot slot, ProjectedRow *buffer, uint16_t offset) {
    uint16_t col_offset = buffer->ColumnOffsets()[offset];
    uint8_t attr_size = accessor.GetBlockLayout().attr_sizes_[col_offset];
    byte *stored_attr = accessor.AccessWithNullCheck(slot, col_offset);

    if (stored_attr == nullptr) {
      buffer->NullBitmap().Set(offset, false);
      return;
    }

    WriteBytes(attr_size, ReadBytes(attr_size, stored_attr), buffer->AttrForceNotNull(offset));
  }
};
}  // namespace terrier::storage