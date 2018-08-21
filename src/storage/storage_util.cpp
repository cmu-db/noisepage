#include <unordered_map>
#include "storage/storage_util.h"
#include "storage/tuple_access_strategy.h"
#include "storage/delta_record.h"

namespace terrier::storage {
void StorageUtil::WriteBytes(const uint8_t attr_size, const uint64_t val, byte *const pos) {
  switch (attr_size) {
    case sizeof(uint8_t):*reinterpret_cast<uint8_t *>(pos) = static_cast<uint8_t>(val);
      break;
    case sizeof(uint16_t):*reinterpret_cast<uint16_t *>(pos) = static_cast<uint16_t>(val);
      break;
    case sizeof(uint32_t):*reinterpret_cast<uint32_t *>(pos) = static_cast<uint32_t>(val);
      break;
    case sizeof(uint64_t):*reinterpret_cast<uint64_t *>(pos) = static_cast<uint64_t>(val);
      break;
    default:
      // Invalid attr size
      throw std::runtime_error("Invalid byte write value");
  }
}

uint64_t StorageUtil::ReadBytes(const uint8_t attr_size, const byte *const pos) {
  switch (attr_size) {
    case sizeof(uint8_t):return *reinterpret_cast<const uint8_t *>(pos);
    case sizeof(uint16_t):return *reinterpret_cast<const uint16_t *>(pos);
    case sizeof(uint32_t):return *reinterpret_cast<const uint32_t *>(pos);
    case sizeof(uint64_t):return *reinterpret_cast<const uint64_t *>(pos);
    default:
      // Invalid attr size
      throw std::runtime_error("Invalid byte write value");
  }
}

void StorageUtil::CopyWithNullCheck(const byte *const from,
                                    ProjectedRow *const to,
                                    const uint8_t size,
                                    const uint16_t col_id) {
  if (from == nullptr)
    to->SetNull(col_id);
  else
    WriteBytes(size, ReadBytes(size, from), to->AccessForceNotNull(col_id));
}

void StorageUtil::CopyWithNullCheck(const byte *const from,
                                    const TupleAccessStrategy &accessor,
                                    const TupleSlot to,
                                    const uint16_t col_id) {
  if (from == nullptr) {
    accessor.SetNull(to, col_id);
  } else {
    uint8_t size = accessor.GetBlockLayout().attr_sizes_[col_id];
    WriteBytes(size, ReadBytes(size, from), accessor.AccessForceNotNull(to, col_id));
  }
}

void StorageUtil::CopyAttrIntoProjection(const TupleAccessStrategy &accessor,
                                         const TupleSlot from,
                                         ProjectedRow *const to,
                                         const uint16_t projection_list_offset) {
  uint16_t col_id = to->ColumnIds()[projection_list_offset];
  uint8_t attr_size = accessor.GetBlockLayout().attr_sizes_[col_id];
  byte *stored_attr = accessor.AccessWithNullCheck(from, col_id);
  CopyWithNullCheck(stored_attr, to, attr_size, projection_list_offset);
}

void StorageUtil::CopyAttrFromProjection(const TupleAccessStrategy &accessor,
                                         const TupleSlot to,
                                         const ProjectedRow &from,
                                         const uint16_t projection_list_offset) {
  uint16_t col_id = from.ColumnIds()[projection_list_offset];
  const byte *stored_attr = from.AccessWithNullCheck(projection_list_offset);
  CopyWithNullCheck(stored_attr, accessor, to, col_id);
}

void StorageUtil::ApplyDelta(const BlockLayout &layout,
                             const ProjectedRow &delta,
                             ProjectedRow *const buffer,
                             const std::unordered_map<uint16_t, uint16_t> &col_to_index) {
  for (uint16_t i = 0; i < delta.NumColumns(); i++) {
    uint16_t delta_col_id = delta.ColumnIds()[i];
    auto it = col_to_index.find(delta_col_id);
    if (it != col_to_index.end()) {
      uint16_t col_id = it->first, buffer_offset = it->second;
      uint8_t attr_size = layout.attr_sizes_[col_id];
      StorageUtil::CopyWithNullCheck(delta.AccessWithNullCheck(i), buffer, attr_size, buffer_offset);
    }
  }
}

void StorageUtil::ApplyDelta(const terrier::storage::BlockLayout &layout,
                             const terrier::storage::ProjectedRow &delta,
                             terrier::storage::ProjectedRow *buffer) {
  std::unordered_map<uint16_t, uint16_t> col_to_index;
  for (uint16_t i = 0; i < buffer->NumColumns(); i++) col_to_index.emplace(buffer->ColumnIds()[i], i);
  ApplyDelta(layout, delta, buffer, col_to_index);
}

uint32_t StorageUtil::PadOffsetToSize(const uint8_t word_size, const uint32_t offset) {
  uint32_t remainder = offset % word_size;
  return remainder == 0 ? offset : offset + word_size - remainder;
}
}  // namespace terrier::storage
