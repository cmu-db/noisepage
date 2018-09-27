#include "storage/storage_util.h"
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"

namespace terrier::storage {
void StorageUtil::WriteBytes(const uint8_t attr_size, const uint64_t val, byte *const pos) {
  switch (attr_size) {
    case sizeof(uint8_t):
      *reinterpret_cast<uint8_t *>(pos) = static_cast<uint8_t>(val);
      break;
    case sizeof(uint16_t):
      *reinterpret_cast<uint16_t *>(pos) = static_cast<uint16_t>(val);
      break;
    case sizeof(uint32_t):
      *reinterpret_cast<uint32_t *>(pos) = static_cast<uint32_t>(val);
      break;
    case sizeof(uint64_t):
      *reinterpret_cast<uint64_t *>(pos) = static_cast<uint64_t>(val);
      break;
    default:
      // Invalid attr size
      throw std::runtime_error("Invalid byte write value");
  }
}

uint64_t StorageUtil::ReadBytes(const uint8_t attr_size, const byte *const pos) {
  switch (attr_size) {
    case sizeof(uint8_t):
      return *reinterpret_cast<const uint8_t *>(pos);
    case sizeof(uint16_t):
      return *reinterpret_cast<const uint16_t *>(pos);
    case sizeof(uint32_t):
      return *reinterpret_cast<const uint32_t *>(pos);
    case sizeof(uint64_t):
      return *reinterpret_cast<const uint64_t *>(pos);
    default:
      // Invalid attr size
      throw std::runtime_error("Invalid byte write value");
  }
}

void StorageUtil::CopyWithNullCheck(const byte *const from, ProjectedRow *const to, const uint8_t size,
                                    const uint16_t projection_list_index) {
  if (from == nullptr)
    to->SetNull(projection_list_index);
  else
    WriteBytes(size, ReadBytes(size, from), to->AccessForceNotNull(projection_list_index));
}

void StorageUtil::CopyWithNullCheck(const byte *const from, const TupleAccessStrategy &accessor, const TupleSlot to,
                                    const col_id_t col_id) {
  if (from == nullptr) {
    accessor.SetNull(to, col_id);
  } else {
    uint8_t size = accessor.GetBlockLayout().AttrSize(col_id);
    WriteBytes(size, ReadBytes(size, from), accessor.AccessForceNotNull(to, col_id));
  }
}

void StorageUtil::CopyAttrIntoProjection(const TupleAccessStrategy &accessor, const TupleSlot from,
                                         ProjectedRow *const to, const uint16_t projection_list_offset) {
  col_id_t col_id = to->ColumnIds()[projection_list_offset];
  uint8_t attr_size = accessor.GetBlockLayout().AttrSize(col_id);
  byte *stored_attr = accessor.AccessWithNullCheck(from, col_id);
  CopyWithNullCheck(stored_attr, to, attr_size, projection_list_offset);
}

void StorageUtil::CopyAttrFromProjection(const TupleAccessStrategy &accessor, const TupleSlot to,
                                         const ProjectedRow &from, const uint16_t projection_list_offset) {
  col_id_t col_id = from.ColumnIds()[projection_list_offset];
  const byte *stored_attr = from.AccessWithNullCheck(projection_list_offset);
  CopyWithNullCheck(stored_attr, accessor, to, col_id);
}

void StorageUtil::ApplyDelta(const BlockLayout &layout, const ProjectedRow &delta, ProjectedRow *const buffer) {
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

std::pair<BlockLayout, ColumnMap> StorageUtil::BlockLayoutFromSchema(const catalog::Schema &schema) {
  uint16_t num_8_byte_attrs = NUM_RESERVED_COLUMNS;
  uint16_t num_4_byte_attrs = 0;
  uint16_t num_2_byte_attrs = 0;
  uint16_t num_1_byte_attrs = 0;

  // Begin with the NUM_RESERVED_COLUMNS in the attr_sizes
  std::vector<uint8_t> attr_sizes({8, 8});

  // First pass through to accumulate the counts of each attr_size
  for (const auto &column : schema.GetColumns()) {
    attr_sizes.push_back(column.GetAttrSize());
    switch (column.GetAttrSize()) {
      case 8:
        num_8_byte_attrs++;
        break;
      case 4:
        num_4_byte_attrs++;
        break;
      case 2:
        num_2_byte_attrs++;
        break;
      case 1:
        num_1_byte_attrs++;
        break;
      default:
        break;
    }
  }

  TERRIER_ASSERT(attr_sizes.size() == num_8_byte_attrs + num_4_byte_attrs + num_2_byte_attrs + num_1_byte_attrs,
                 "Number of attr_sizes does not match the sum of attr counts.");

  // Initialize the offsets for each attr_size
  uint16_t offset_8_byte_attrs = NUM_RESERVED_COLUMNS;
  uint16_t offset_4_byte_attrs = num_8_byte_attrs;
  uint16_t offset_2_byte_attrs = offset_4_byte_attrs + num_4_byte_attrs;
  uint16_t offset_1_byte_attrs = offset_2_byte_attrs + num_2_byte_attrs;

  ColumnMap col_oid_to_id;
  // Build the map from Schema columns to underlying columns
  for (const auto &column : schema.GetColumns()) {
    switch (column.GetAttrSize()) {
      case 8:
        col_oid_to_id[column.GetOid()] = col_id_t(offset_8_byte_attrs++);
        break;
      case 4:
        col_oid_to_id[column.GetOid()] = col_id_t(offset_4_byte_attrs++);
        break;
      case 2:
        col_oid_to_id[column.GetOid()] = col_id_t(offset_2_byte_attrs++);
        break;
      case 1:
        col_oid_to_id[column.GetOid()] = col_id_t(offset_1_byte_attrs++);
        break;
      default:
        break;
    }
  }

  return {storage::BlockLayout(attr_sizes), col_oid_to_id};
}

}  // namespace terrier::storage
