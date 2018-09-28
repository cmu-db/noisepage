#include "storage/storage_util.h"
#include <unordered_map>
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"
#include "storage/materialized_columns.h"

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

template<class RowType>
void StorageUtil::CopyWithNullCheck(const byte *const from, RowType *const to, const uint8_t size,
                                    const uint16_t projection_list_index) {
  if (from == nullptr)
    to->SetNull(projection_list_index);
  else
    WriteBytes(size, ReadBytes(size, from), to->AccessForceNotNull(projection_list_index));
}

template void StorageUtil::CopyWithNullCheck<ProjectedRow>(const byte *, ProjectedRow *, uint8_t, uint16_t);
template void StorageUtil::CopyWithNullCheck<MaterializedColumns::RowView>(const byte *,
                                                                           MaterializedColumns::RowView *,
                                                                           uint8_t,
                                                                           uint16_t);

void StorageUtil::CopyWithNullCheck(const byte *const from, const TupleAccessStrategy &accessor, const TupleSlot to,
                                    const col_id_t col_id) {
  if (from == nullptr) {
    accessor.SetNull(to, col_id);
  } else {
    uint8_t size = accessor.GetBlockLayout().AttrSize(col_id);
    WriteBytes(size, ReadBytes(size, from), accessor.AccessForceNotNull(to, col_id));
  }
}

template<class RowType>
void StorageUtil::CopyAttrIntoProjection(const TupleAccessStrategy &accessor, const TupleSlot from,
                                         RowType *const to, const uint16_t projection_list_offset) {
  col_id_t col_id = to->ColumnIds()[projection_list_offset];
  uint8_t attr_size = accessor.GetBlockLayout().AttrSize(col_id);
  byte *stored_attr = accessor.AccessWithNullCheck(from, col_id);
  CopyWithNullCheck(stored_attr, to, attr_size, projection_list_offset);
}

template void StorageUtil::CopyAttrIntoProjection<ProjectedRow>(const TupleAccessStrategy &,
                                                                TupleSlot,
                                                                ProjectedRow *,
                                                                uint16_t);
template void StorageUtil::CopyAttrIntoProjection<MaterializedColumns::RowView>(const TupleAccessStrategy &,
                                                                                TupleSlot,
                                                                                MaterializedColumns::RowView *,
                                                                                uint16_t);

template<class RowType>
void StorageUtil::CopyAttrFromProjection(const TupleAccessStrategy &accessor, const TupleSlot to,
                                         const RowType &from, const uint16_t projection_list_offset) {
  col_id_t col_id = from.ColumnIds()[projection_list_offset];
  const byte *stored_attr = from.AccessWithNullCheck(projection_list_offset);
  CopyWithNullCheck(stored_attr, accessor, to, col_id);
}

template void StorageUtil::CopyAttrFromProjection<ProjectedRow>(const TupleAccessStrategy &,
                                                                TupleSlot,
                                                                const ProjectedRow &,
                                                                uint16_t);
template void StorageUtil::CopyAttrFromProjection<MaterializedColumns::RowView>(const TupleAccessStrategy &,
                                                                                TupleSlot,
                                                                                const MaterializedColumns::RowView &,
                                                                                uint16_t);

template<class RowType>
void StorageUtil::ApplyDelta(const BlockLayout &layout, const ProjectedRow &delta, RowType *const buffer) {
  // the projection list in delta and buffer have to be sorted in the same way for this to work,
  // which should be guaranteed if both are constructed correctly using ProjectedRowInitializer,
  // (or copied from a valid ProjectedRow)
  uint16_t delta_i = 0, buffer_i = 0;
  while (delta_i < delta.NumColumns() && buffer_i < buffer->NumColumns()) {
    col_id_t delta_col_id = delta.ColumnIds()[delta_i], buffer_col_id = buffer->ColumnIds()[buffer_i];
    if (delta_col_id == buffer_col_id) {
      // Should apply changes
      TERRIER_ASSERT(delta_col_id != PRESENCE_COLUMN_ID,
                     "Output buffer should never return the version vector column.");
      TERRIER_ASSERT(delta_col_id != LOGICAL_DELETE_COLUMN_ID,
                     "Output buffer should never return the logical delete column.");
      uint8_t attr_size = layout.AttrSize(delta_col_id);
      StorageUtil::CopyWithNullCheck(delta.AccessWithNullCheck(delta_i), buffer, attr_size, buffer_i);
      delta_i++;
      buffer_i++;
    } else if (delta_col_id > buffer_col_id) {
      // buffer is behind
      buffer_i++;
    } else {
      // delta is behind
      delta_i++;
    }
  }
}

template void StorageUtil::ApplyDelta<ProjectedRow>(const BlockLayout &layout,
                                                    const ProjectedRow &delta,
                                                    ProjectedRow *buffer);
template void StorageUtil::ApplyDelta<MaterializedColumns::RowView>(const BlockLayout &layout,
                                                                    const ProjectedRow &delta,
                                                                    MaterializedColumns::RowView *buffer);

DeltaRecordType StorageUtil::CheckUndoRecordType(const UndoRecord &undo) {
  const ProjectedRow &delta = *(undo.Delta());
  if (delta.ColumnIds()[0] == LOGICAL_DELETE_COLUMN_ID) {
    if (delta.IsNull(0)) {
      return DeltaRecordType::INSERT;
    }
    return DeltaRecordType::DELETE;
  }
  return DeltaRecordType::UPDATE;
}

uint32_t StorageUtil::PadUpToSize(const uint8_t word_size, const uint32_t offset) {
  const uint32_t remainder = offset % word_size;
  return remainder == 0 ? offset : offset + word_size - remainder;
}
}  // namespace terrier::storage
